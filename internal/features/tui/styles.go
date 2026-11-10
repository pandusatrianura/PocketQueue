package tui

import (
	"github.com/charmbracelet/lipgloss"
)

var (
	// Colors
	ColorPrimary   = lipgloss.Color("#7D56F4")
	ColorSecondary = lipgloss.Color("#00D2FF")
	ColorSuccess   = lipgloss.Color("#04B575")
	ColorWarning   = lipgloss.Color("#FFAE00")
	ColorDanger    = lipgloss.Color("#FF5F87")
	ColorMuted     = lipgloss.Color("#626262")
	ColorBgDark    = lipgloss.Color("#1A1A24")
	ColorCardBg    = lipgloss.Color("#222230")

	// Header Styles
	StyleTitle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(ColorPrimary).
			Padding(0, 1)

	StyleSubtitle = lipgloss.NewStyle().
			Foreground(ColorMuted).
			MarginLeft(1)

	StyleActiveTab = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(ColorPrimary).
			Padding(0, 2)

	StyleInactiveTab = lipgloss.NewStyle().
				Foreground(ColorMuted).
				Padding(0, 2)

	// Status / Badge Styles
	StyleBadgeDLQ = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(ColorDanger).
			Padding(0, 1)

	StyleBadgeOK = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(ColorSuccess).
			Padding(0, 1)

	// Modal / Card Styles
	StyleModal = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(ColorPrimary).
			Padding(1, 2)

	StyleStatusBar = lipgloss.NewStyle().
			Foreground(ColorMuted).
			MarginTop(1)

	StyleKeyHelp = lipgloss.NewStyle().
			Foreground(ColorSecondary).
			Bold(true)
)

