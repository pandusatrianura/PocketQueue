package tui_test

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
	"github.com/pandusatrianura/pocketqueue/internal/features/tui"
)

func TestAppModel_Lifecycle(t *testing.T) {
	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(50)
	svc := broker.NewService(engine, bus, 1)

	model := tui.NewAppModel(svc, bus, 100)

	// 1. Initial view contains banner
	initialView := model.View()
	if !strings.Contains(initialView, "POCKETQUEUE") {
		t.Errorf("expected view to contain 'POCKETQUEUE', got: %s", initialView)
	}

	// 2. Simulate window resize
	updatedModel, _ := model.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	model = updatedModel.(*tui.AppModel)

	// 3. Send record arrived message
	rec := storage.Record{
		Topic:     "events",
		Partition: 0,
		Offset:    0,
		Key:       []byte("evt-1"),
		Value:     []byte(`{"status":"ok"}`),
		Timestamp: time.Now(),
	}

	// Send record via eventbus
	bus.Publish(rec)

	// 4. Test key navigation: tab to switch view
	updatedModel, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	model = updatedModel.(*tui.AppModel)

	topicView := model.View()
	if !strings.Contains(topicView, "2: Topics & DLQ") {
		t.Errorf("expected view to indicate topic tab, got: %s", topicView)
	}

	// Tab back
	updatedModel, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	model = updatedModel.(*tui.AppModel)

	// 5. Test Quit
	_, cmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	if cmd == nil {
		t.Errorf("expected quit cmd on 'q', got nil")
	}
}

