package ui

import (
	razpravljalnica "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/rivo/tview"
)

func newSidebar(d *Display) tview.Primitive {

	table := tview.NewTable()

	topics := d.conn.GetTopics()

	for i, topic := range topics {
		addSidebarTopicEntry(table, topic, i)
	}

	return table.Select(0, 0).SetSelectable(true, false).SetSelectedFunc(func(row, column int) {
		topic := table.GetCell(row, column).GetReference().(*razpravljalnica.Topic)
		d.ToMessageList(topic)
	})
}

func addSidebarTopicEntry(table *tview.Table, topic *razpravljalnica.Topic, row int) {
	cell := tview.NewTableCell(topic.GetName()).SetReference(topic)
	table.SetCell(row, 0, cell)
}
