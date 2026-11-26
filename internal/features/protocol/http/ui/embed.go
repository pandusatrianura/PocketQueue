package ui

import (
	"embed"
	"io/fs"
)

//go:embed assets/*
var AssetsFS embed.FS

// ReadAsset reads a static asset from the embedded assets directory.
func ReadAsset(name string) ([]byte, error) {
	return AssetsFS.ReadFile("assets/" + name)
}

// SubFS returns an fs.FS rooted at assets.
func SubFS() (fs.FS, error) {
	return fs.Sub(AssetsFS, "assets")
}
