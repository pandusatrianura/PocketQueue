package http

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/http/ui"
)

var (
	gzipPool = sync.Pool{
		New: func() any {
			return gzip.NewWriter(nil)
		},
	}
)

func (h *Handler) handleServeUI(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	content, err := ui.ReadAsset("index.html")
	if err != nil {
		http.Error(w, "failed to load dashboard: "+err.Error(), http.StatusInternalServerError)
		return
	}

	etag := fmt.Sprintf("\"%x\"", sha256.Sum256(content))
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "no-cache")

	if match := r.Header.Get("If-None-Match"); match != "" && (match == etag || match == "*") {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	h.serveContentWithGzip(w, r, "text/html; charset=utf-8", content)
}

func (h *Handler) handleServeStatic(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	file := r.PathValue("file")
	if file == "" || strings.Contains(file, "..") || strings.Contains(file, "/") || strings.Contains(file, "\\") {
		http.NotFound(w, r)
		return
	}

	content, err := ui.ReadAsset(file)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	mimeType := detectMIMEType(file)
	etag := fmt.Sprintf("\"%x\"", sha256.Sum256(content))
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "public, max-age=86400")

	if match := r.Header.Get("If-None-Match"); match != "" && (match == etag || match == "*") {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	h.serveContentWithGzip(w, r, mimeType, content)
}

func (h *Handler) serveContentWithGzip(w http.ResponseWriter, r *http.Request, contentType string, content []byte) {
	w.Header().Set("Content-Type", contentType)

	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Vary", "Accept-Encoding")
		w.WriteHeader(http.StatusOK)

		var buf bytes.Buffer
		gz := gzipPool.Get().(*gzip.Writer)
		gz.Reset(&buf)
		_, _ = gz.Write(content)
		_ = gz.Close()
		gzipPool.Put(gz)

		_, _ = w.Write(buf.Bytes())
		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

func detectMIMEType(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".css":
		return "text/css; charset=utf-8"
	case ".js":
		return "application/javascript; charset=utf-8"
	case ".svg":
		return "image/svg+xml"
	case ".html", ".htm":
		return "text/html; charset=utf-8"
	case ".json":
		return "application/json"
	case ".png":
		return "image/png"
	case ".ico":
		return "image/x-icon"
	default:
		return "application/octet-stream"
	}
}
