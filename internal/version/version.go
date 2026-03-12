package version

// Эти переменные заполняются через ldflags при сборке
var (
	// Version содержит номер версии (задается при сборке)
	Version = "dev"

	// Commit содержит хеш коммита (задается при сборке)
	Commit = "none"

	// BuildTime содержит время сборки (задается при сборке)
	BuildTime = "unknown"
)

// Info возвращает строку с полной информацией о версии
func Info() string {
	return "GeoStreamer " + Version + " (commit: " + Commit + ", built: " + BuildTime + ")"
}

// Short возвращает краткую версию
func Short() string {
	return Version
}
