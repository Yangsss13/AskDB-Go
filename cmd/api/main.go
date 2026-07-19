package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Yangsss13/askdb-go/internal/auth"
	"github.com/Yangsss13/askdb-go/internal/config"
	"github.com/Yangsss13/askdb-go/internal/crypto"
	"github.com/Yangsss13/askdb-go/internal/datasource"
	"github.com/Yangsss13/askdb-go/internal/handler"
	"github.com/Yangsss13/askdb-go/internal/infra"
	"github.com/Yangsss13/askdb-go/internal/middleware"
	"github.com/Yangsss13/askdb-go/internal/netguard"
	"github.com/Yangsss13/askdb-go/internal/queryjob"
	"github.com/Yangsss13/askdb-go/internal/queryresult"
	"github.com/Yangsss13/askdb-go/internal/user"
)

// nullRabbitChecker satisfies handler.HealthDeps.Rabbit when no MQ connection
// was established at startup. It always reports unhealthy so /readyz returns 503.
type nullRabbitChecker struct{}

func (nullRabbitChecker) IsHealthy() bool { return false }

// rabbitChecker abstracts the RabbitMQ health check for /readyz.
type rabbitChecker interface {
	IsHealthy() bool
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "err", err)
		os.Exit(1)
	}

	// JWT is required for the API only; the worker starts without it.
	if err := cfg.ValidateJWT(); err != nil {
		slog.Error("jwt config invalid", "err", err)
		os.Exit(1)
	}

	// DATA_SOURCE_KEY is required by both API (encrypt on write) and Worker (decrypt on read).
	if err := cfg.ValidateDataSourceKey(); err != nil {
		slog.Error("data source key invalid", "err", err)
		os.Exit(1)
	}

	// --- infrastructure ---
	db, err := infra.NewMySQL(cfg.MySQLDSN)
	if err != nil {
		slog.Error("mysql init failed", "err", err)
		os.Exit(1)
	}

	readerDB, err := infra.NewReaderDB(cfg.MySQLReaderDSN)
	if err != nil {
		slog.Error("reader db init failed", "err", err)
		os.Exit(1)
	}

	rdb, err := infra.NewRedis(cfg.RedisAddr, cfg.RedisPass)
	if err != nil {
		slog.Error("redis init failed", "err", err)
		os.Exit(1)
	}

	// RabbitMQ is not a hard startup dependency for the API. The Dispatcher
	// reconnects in the background when the broker becomes available.
	// /readyz reports rabbitmq=unreachable until connectivity is established.
	var mq *infra.RabbitMQ
	var rabbit rabbitChecker = nullRabbitChecker{}
	if mqInst, mqErr := infra.NewRabbitMQ(cfg.RabbitMQURL); mqErr != nil {
		slog.Warn("rabbitmq init failed; dispatcher will reconnect in background", "err", mqErr)
	} else {
		mq = mqInst
		rabbit = mq
	}

	// --- crypto cipher ---
	cipher, err := crypto.NewCipher(cfg.DataSourceKey)
	if err != nil {
		slog.Error("crypto cipher init failed", "err", err)
		os.Exit(1)
	}

	// --- network guard ---
	allowedPorts, err := netguard.ParseAllowedPorts(cfg.AllowedDBPorts)
	if err != nil {
		slog.Error("netguard: invalid ALLOWED_DB_PORTS", "err", err)
		os.Exit(1)
	}
	guard, err := netguard.NewValidator(netguard.Config{
		AllowedPorts:     allowedPorts,
		PrivateAllowlist: netguard.ParsePrivateAllowlist(cfg.PrivateHostAllowlist),
	})
	if err != nil {
		slog.Error("netguard: validator init failed", "err", err)
		os.Exit(1)
	}

	// --- data-source wiring ---
	dsRepo := datasource.NewGORMRepository(db.GORM)
	dsSvc := datasource.NewService(dsRepo, cipher, guard)
	dsHandler := handler.NewDataSourceHandler(dsSvc)

	// --- auth wiring ---
	jwtMgr := auth.NewJWTManager(cfg.JWTSecret, cfg.JWTIssuer, cfg.JWTAccessTTL)
	userRepo := user.NewGORMRepository(db.GORM)
	authSvc := user.NewAuthService(userRepo, jwtMgr)
	authHandler := user.NewAuthHandler(authSvc)

	// --- query-job wiring (Phase 8: Outbox replaces direct publisher) ---
	repo := queryjob.NewGORMRepository(db.GORM)
	outboxRepo := queryjob.NewGORMOutboxRepository(db.GORM)
	resultStore := queryresult.NewRedisStore(rdb)
	queryService := queryjob.NewService(repo, outboxRepo, dsRepo)
	resultService := queryjob.NewResultService(repo, resultStore)
	queryHandler := handler.NewQueryJobHandler(queryService, resultService)

	// --- Outbox Dispatcher (runs in API background; owns its own MQ connection) ---
	dispatcher := queryjob.NewDispatcher(outboxRepo, cfg.RabbitMQURL, queryjob.DispatcherConfig{
		PollInterval:    cfg.OutboxPollInterval,
		BatchSize:       cfg.OutboxBatchSize,
		LeaseTTL:        cfg.OutboxLeaseTTL,
		BaseBackoff:     cfg.OutboxBaseBackoff,
		MaxBackoff:      cfg.OutboxMaxBackoff,
		PublishedRetain: cfg.OutboxPublishedRetain,
		CleanBatch:      cfg.OutboxCleanBatch,
		ConfirmTimeout:  cfg.MQConfirmTimeout,
	})
	dispatcher.Start()

	// --- routes ---
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", handler.Healthz)
	r.GET("/readyz", handler.Readyz(handler.HealthDeps{
		MySQL:  db,
		Redis:  rdb,
		Rabbit: rabbit,
	}))

	// Public auth routes — no Bearer middleware.
	v1 := r.Group("/api/v1")
	auth1 := v1.Group("/auth")
	{
		auth1.POST("/register", authHandler.Register)
		auth1.POST("/login", authHandler.Login)
	}

	// Protected routes — Bearer middleware enforces authentication.
	protected := v1.Group("/", middleware.Bearer(jwtMgr))
	{
		protected.POST("/query-jobs", queryHandler.Submit)
		protected.GET("/query-jobs/:id", queryHandler.Get)
		protected.GET("/query-jobs/:id/result", queryHandler.GetResult)

		// Data-source management.
		ds := protected.Group("/data-sources")
		ds.POST("", dsHandler.Create)
		ds.GET("", dsHandler.List)
		ds.GET("/:id", dsHandler.GetByID)
		ds.PUT("/:id", dsHandler.Update)
		ds.DELETE("/:id", dsHandler.Delete)
		ds.POST("/:id/test", dsHandler.TestConnection)
	}

	srv := &http.Server{
		Addr:    ":" + cfg.APIPort,
		Handler: r,
	}

	// --- graceful shutdown ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		slog.Info("api: listening", "port", cfg.APIPort)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("api: server error", "err", err)
			os.Exit(1)
		}
	}()

	<-quit
	slog.Info("api: shutdown signal received")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("api: shutdown error", "err", err)
	}

	// Stop dispatcher: wait for in-flight publishes, then release unclaimed events.
	dispatchCtx, dispatchCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dispatchCancel()
	dispatcher.Stop(dispatchCtx)

	// Close infrastructure in reverse-init order.
	if mq != nil {
		if err := mq.Close(); err != nil {
			slog.Error("rabbitmq: close error", "err", err)
		}
	}
	if err := rdb.Close(); err != nil {
		slog.Error("redis: close error", "err", err)
	}
	if err := readerDB.Close(); err != nil {
		slog.Error("reader db: close error", "err", err)
	}
	if err := db.Close(); err != nil {
		slog.Error("mysql: close error", "err", err)
	}

	slog.Info("api: shutdown complete")
}
