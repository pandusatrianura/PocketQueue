package tui

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

type tabIndex int

const (
	tabStream tabIndex = iota
	tabTopics
)

type modalMode int

const (
	modalNone modalMode = iota
	modalInspect
	modalReplay
)

type recordArrivedMsg storage.Record

// AppModel is the primary Charm Bubbletea application state model.
type AppModel struct {
	broker   *broker.Service
	eventBus *broker.EventBus
	subCh    <-chan storage.Record
	unsub    func()

	// UI Layout & Dimensions
	width  int
	height int

	// Navigation
	activeTab tabIndex
	modal     modalMode

	// Live Stream State
	messages []storage.Record
	maxMsgs  int
	table    table.Model

	// Topics View State
	topicTable table.Model

	// Inspector / Replay Modals
	selectedMsg *storage.Record
	replayText  textarea.Model
	statusMsg   string
}

// NewAppModel creates a new Bubbletea model connected to the broker.
func NewAppModel(b *broker.Service, eb *broker.EventBus, maxMessages int) *AppModel {
	if maxMessages <= 0 {
		maxMessages = 1000
	}

	var subCh <-chan storage.Record
	var unsub func()
	if eb != nil {
		subCh, unsub = eb.Subscribe()
	}

	// Columns for Stream table
	cols := []table.Column{
		{Title: "TIME", Width: 10},
		{Title: "TOPIC", Width: 18},
		{Title: "PART", Width: 6},
		{Title: "OFFSET", Width: 8},
		{Title: "KEY", Width: 15},
		{Title: "PAYLOAD PREVIEW", Width: 40},
	}

	t := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithHeight(15),
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(ColorPrimary).
		BorderBottom(true).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("#FFFFFF")).
		Background(ColorPrimary).
		Bold(true)
	t.SetStyles(s)

	// Columns for Topic Table
	topCols := []table.Column{
		{Title: "TOPIC NAME", Width: 25},
		{Title: "PARTITIONS", Width: 12},
		{Title: "TOTAL MSGS", Width: 12},
		{Title: "EARLIEST", Width: 10},
		{Title: "LATEST", Width: 10},
		{Title: "STATUS", Width: 10},
	}
	topTable := table.New(
		table.WithColumns(topCols),
		table.WithFocused(true),
		table.WithHeight(15),
	)
	topTable.SetStyles(s)

	ta := textarea.New()
	ta.Placeholder = "Edit payload JSON here..."
	ta.SetWidth(60)
	ta.SetHeight(10)

	return &AppModel{
		broker:      b,
		eventBus:    eb,
		subCh:       subCh,
		unsub:       unsub,
		activeTab:   tabStream,
		modal:       modalNone,
		messages:    make([]storage.Record, 0, maxMessages),
		maxMsgs:     maxMessages,
		table:       t,
		topicTable:  topTable,
		replayText:  ta,
		statusMsg:   "PocketQueue Live • Port 9092",
	}
}

// waitForNextRecord creates a Bubbletea command listening on the event bus subscription.
func (m *AppModel) waitForNextRecord() tea.Cmd {
	return func() tea.Msg {
		if m.subCh == nil {
			return nil
		}
		rec, ok := <-m.subCh
		if !ok {
			return nil
		}
		return recordArrivedMsg(rec)
	}
}

// Init starts listening for events.
func (m *AppModel) Init() tea.Cmd {
	return m.waitForNextRecord()
}

// Update processes incoming Bubbletea messages and keystrokes.
func (m *AppModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.updateSizes()

	case recordArrivedMsg:
		rec := storage.Record(msg)
		if len(m.messages) >= m.maxMsgs {
			m.messages = append(m.messages[1:], rec)
		} else {
			m.messages = append(m.messages, rec)
		}
		m.refreshStreamRows()
		m.refreshTopicRows()
		return m, m.waitForNextRecord()

	case tea.KeyMsg:
		switch m.modal {
		case modalInspect:
			if msg.String() == "esc" || msg.String() == "enter" || msg.String() == "q" {
				m.modal = modalNone
				return m, nil
			}
			if msg.String() == "r" || msg.String() == "e" {
				m.openReplayModal()
				return m, nil
			}

		case modalReplay:
			if msg.String() == "esc" {
				m.modal = modalNone
				return m, nil
			}
			if msg.String() == "ctrl+s" {
				m.executeReplay()
				m.modal = modalNone
				return m, nil
			}
			var taCmd tea.Cmd
			m.replayText, taCmd = m.replayText.Update(msg)
			return m, taCmd

		case modalNone:
			switch msg.String() {
			case "q", "ctrl+c":
				if m.unsub != nil {
					m.unsub()
				}
				return m, tea.Quit
			case "tab":
				if m.activeTab == tabStream {
					m.activeTab = tabTopics
					m.refreshTopicRows()
				} else {
					m.activeTab = tabStream
				}
				return m, nil
			case "enter":
				m.openInspectModal()
				return m, nil
			case "r", "e":
				m.openReplayModal()
				return m, nil
			}
		}
	}

	if m.modal == modalNone {
		if m.activeTab == tabStream {
			m.table, cmd = m.table.Update(msg)
		} else {
			m.topicTable, cmd = m.topicTable.Update(msg)
		}
	}

	return m, cmd
}

func (m *AppModel) updateSizes() {
	tableHeight := m.height - 7
	if tableHeight < 5 {
		tableHeight = 5
	}
	m.table.SetHeight(tableHeight)
	m.topicTable.SetHeight(tableHeight)

	previewColWidth := m.width - 65
	if previewColWidth < 20 {
		previewColWidth = 20
	}

	cols := m.table.Columns()
	if len(cols) == 6 {
		cols[5].Width = previewColWidth
		m.table.SetColumns(cols)
	}

	m.replayText.SetWidth(m.width - 20)
}

func (m *AppModel) refreshStreamRows() {
	rows := make([]table.Row, len(m.messages))
	for i, r := range m.messages {
		timeStr := r.Timestamp.Format("15:04:05")
		keyStr := string(r.Key)
		if keyStr == "" {
			keyStr = "-"
		}

		// Preview truncated
		preview := strings.ReplaceAll(string(r.Value), "\n", " ")
		if len(preview) > 50 {
			preview = preview[:47] + "..."
		}

		topicDisplay := r.Topic
		if r.IsDLQ || strings.HasSuffix(r.Topic, ".dlq") {
			topicDisplay = "🚨 " + r.Topic
		}

		rows[i] = table.Row{
			timeStr,
			topicDisplay,
			fmt.Sprintf("%d", r.Partition),
			fmt.Sprintf("%d", r.Offset),
			keyStr,
			preview,
		}
	}
	m.table.SetRows(rows)
	// Auto scroll to latest if selected at end
	if len(rows) > 0 && m.table.Cursor() >= len(rows)-2 {
		m.table.SetCursor(len(rows) - 1)
	}
}

func (m *AppModel) refreshTopicRows() {
	if m.broker == nil {
		return
	}
	topics, err := m.broker.ListTopics(context.Background())
	if err != nil {
		return
	}

	rows := make([]table.Row, 0, len(topics))
	for tName, partCount := range topics {
		meta, err := m.broker.GetTopicMetadata(context.Background(), tName)
		var earliest, latest, total int64
		if err == nil && len(meta.Partitions) > 0 {
			for _, p := range meta.Partitions {
				earliest = p.Earliest
				latest = p.Latest
				total += p.TotalCount
			}
		}

		status := "Active"
		if strings.HasSuffix(tName, ".dlq") {
			status = "🚨 DLQ"
		}

		rows = append(rows, table.Row{
			tName,
			fmt.Sprintf("%d", partCount),
			fmt.Sprintf("%d", total),
			fmt.Sprintf("%d", earliest),
			fmt.Sprintf("%d", latest),
			status,
		})
	}
	m.topicTable.SetRows(rows)
}

func (m *AppModel) openInspectModal() {
	idx := m.table.Cursor()
	if idx >= 0 && idx < len(m.messages) {
		m.selectedMsg = &m.messages[idx]
		m.modal = modalInspect
	}
}

func (m *AppModel) openReplayModal() {
	idx := m.table.Cursor()
	if idx >= 0 && idx < len(m.messages) {
		m.selectedMsg = &m.messages[idx]
		m.replayText.SetValue(string(m.selectedMsg.Value))
		m.replayText.Focus()
		m.modal = modalReplay
	}
}

func (m *AppModel) executeReplay() {
	if m.selectedMsg == nil || m.broker == nil {
		return
	}
	targetTopic := strings.TrimSuffix(m.selectedMsg.Topic, ".dlq")
	newPayload := []byte(m.replayText.Value())

	_, err := m.broker.Replay(context.Background(), targetTopic, m.selectedMsg.Partition, m.selectedMsg.Key, newPayload, m.selectedMsg.Headers)
	if err != nil {
		m.statusMsg = fmt.Sprintf("❌ Replay failed: %v", err)
	} else {
		m.statusMsg = fmt.Sprintf("✅ Replayed to topic %s at %s", targetTopic, time.Now().Format("15:04:05"))
	}
}

// View renders the TUI interface.
func (m *AppModel) View() string {
	var sb strings.Builder

	// 1. Header Bar
	header := lipgloss.JoinHorizontal(lipgloss.Center,
		StyleTitle.Render("⚡ POCKETQUEUE"),
		StyleSubtitle.Render("Ultra-Lightweight Embedded Broker (<15MB, <25MB RAM)"),
	)
	sb.WriteString(header + "\n\n")

	// 2. Tabs
	streamTab := StyleInactiveTab.Render("1: Live Stream")
	topicsTab := StyleInactiveTab.Render("2: Topics & DLQ")
	if m.activeTab == tabStream {
		streamTab = StyleActiveTab.Render("1: Live Stream")
	} else {
		topicsTab = StyleActiveTab.Render("2: Topics & DLQ")
	}
	sb.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, streamTab, topicsTab) + "\n\n")

	// 3. Main Content or Modal
	switch m.modal {
	case modalInspect:
		sb.WriteString(m.renderInspectModal())
	case modalReplay:
		sb.WriteString(m.renderReplayModal())
	default:
		if m.activeTab == tabStream {
			if len(m.messages) == 0 {
				emptyNotice := lipgloss.NewStyle().
					Foreground(ColorMuted).
					Padding(2, 4).
					Render("No events yet. Point your Kafka client to localhost:9092 to stream messages in real-time.")
				sb.WriteString(emptyNotice + "\n")
			} else {
				sb.WriteString(m.table.View() + "\n")
			}
		} else {
			sb.WriteString(m.topicTable.View() + "\n")
		}
	}

	// 4. Status Bar & Key Help
	keyHelp := "Tab: Switch View • Enter: Inspect • r/e: Replay & Edit • q: Quit"
	if m.modal == modalInspect {
		keyHelp = "Esc/Enter: Close • r/e: Edit & Replay"
	} else if m.modal == modalReplay {
		keyHelp = "Ctrl+S: Submit Replay • Esc: Cancel"
	}

	sb.WriteString("\n" + StyleStatusBar.Render(fmt.Sprintf("%s | %s", m.statusMsg, keyHelp)))

	return sb.String()
}

func (m *AppModel) renderInspectModal() string {
	if m.selectedMsg == nil {
		return ""
	}

	var jsonPretty bytes.Buffer
	if err := json.Indent(&jsonPretty, m.selectedMsg.Value, "", "  "); err == nil {
		// Pretty json
	} else {
		jsonPretty.Write(m.selectedMsg.Value)
	}

	headersStr := "-"
	if len(m.selectedMsg.Headers) > 0 {
		hList := make([]string, len(m.selectedMsg.Headers))
		for i, h := range m.selectedMsg.Headers {
			hList[i] = fmt.Sprintf("%s=%s", h.Key, string(h.Value))
		}
		headersStr = strings.Join(hList, ", ")
	}

	content := fmt.Sprintf(
		"TOPIC:     %s\nPARTITION: %d\nOFFSET:    %d\nTIME:      %s\nKEY:       %s\nHEADERS:   %s\n\nPAYLOAD:\n%s",
		m.selectedMsg.Topic,
		m.selectedMsg.Partition,
		m.selectedMsg.Offset,
		m.selectedMsg.Timestamp.Format(time.RFC3339),
		string(m.selectedMsg.Key),
		headersStr,
		jsonPretty.String(),
	)

	return StyleModal.Render(content)
}

func (m *AppModel) renderReplayModal() string {
	if m.selectedMsg == nil {
		return ""
	}

	targetTopic := strings.TrimSuffix(m.selectedMsg.Topic, ".dlq")
	header := fmt.Sprintf("REPLAY STUDIO (Target Topic: %s)\nEdit payload and press Ctrl+S to republish:\n\n", targetTopic)

	return StyleModal.Render(header + m.replayText.View())
}

