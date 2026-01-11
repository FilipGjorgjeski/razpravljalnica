package gui

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const (
	colorText = tcell.ColorWhite
	colorBg   = tcell.ColorBlack

	colorHighlight = tcell.ColorRed

	colorSelectedText = tcell.ColorBlack
	colorSelectedBg   = tcell.ColorSlateGray

	colorHighlightedFg = tcell.ColorBlack
	colorHighlightedBg = tcell.ColorGray

	colorMutedText = tcell.ColorGray

	colorFormText = tcell.ColorWhite
	colorFormBg   = tcell.ColorSlateGray
)

func StyleNormalTableCell(cell *tview.TableCell) *tview.TableCell {
	return cell.
		SetTextColor(colorText).
		SetBackgroundColor(colorBg).
		SetAttributes(tcell.AttrNone)
}

func StyleMutedTableCell(cell *tview.TableCell) *tview.TableCell {
	return cell.
		SetTextColor(colorMutedText).
		SetBackgroundColor(colorBg)
}

func StyleSelectedTableCell(cell *tview.TableCell) *tview.TableCell {
	return cell.
		SetTextColor(colorSelectedText).
		SetBackgroundColor(colorSelectedBg).
		SetAttributes(tcell.AttrBold)
}

func StyleNormalTable(table *tview.Table) *tview.Table {
	return table.
		SetSelectedStyle(tcell.StyleDefault.Background(colorHighlightedBg).Foreground(colorHighlightedFg))
}

func StyleNormalForm(form *tview.Form) *tview.Form {
	return form.
		SetFieldBackgroundColor(colorFormBg).
		SetFieldTextColor(colorFormText).
		SetButtonBackgroundColor(colorFormBg).
		SetButtonTextColor(colorFormText)
}
