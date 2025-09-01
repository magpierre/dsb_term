package main

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/gdamore/tcell/v2"
	delta_sharing "github.com/magpierre/go_delta_sharing_client"
	"github.com/rivo/tview"
)

type retval struct {
	Type     string
	Nullable string
	Metadata string
}

func setSelectionChangedFunction(t *tview.Table, stats *tview.Table, sparkSchema *tview.Table) {
	t.SetSelectionChangedFunc(func(row, column int) {
		tc := t.GetCell(0, column)
		value := tc.Text

		for i := 1; i < stats.GetRowCount(); i++ {
			if stats.GetCell(i, 0).Text == value {
				stats.Select(i, 0)
				break
			}
		}
		for i := 1; i < sparkSchema.GetRowCount(); i++ {
			if sparkSchema.GetCell(i, 0).Text == value {
				sparkSchema.Select(i, 0)
				break
			} else {
				sparkSchema.Select(0, 0)
			}
		}
	})
}

func renderResult(
	done chan interface{},
	app *tview.Application,
	results *tview.Table,
	client interface{},
	table delta_sharing.Table,
	fileId string,
) {
	results.Clear()
	results.SetBorders(true)
	defer close(done)

	t, err := delta_sharing.LoadArrowTable(client, table, fileId)
	if err != nil {
		return
	}

	tr := array.NewTableReader(t, 1000)
	tr.Retain()
	for i := 0; i < int(t.NumCols()); i++ {
		c := tview.NewTableCell(t.Column(i).Name()).SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(0)

		results.SetCell(0, i, c)
	}

	tr.Next()
	rec := tr.Record()
	for pos := 0; pos < int(rec.NumRows()); pos++ {
		for i, col := range rec.Columns() {
			switch col.DataType().ID() {
			case arrow.STRUCT:
				a := col.(*array.Struct)
				fmt.Println(a.Field(0).DataType().ID())

				results.SetCell(pos+1, i, tview.NewTableCell(a.Field(1).String()).SetExpansion(4).SetMaxWidth(32).SetAlign(0))
			case arrow.STRING:
				a := col.(*array.String)
				results.SetCell(pos+1, i, tview.NewTableCell(a.Value(pos)).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.INT16:
				i16 := col.(*array.Int16)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%d", i16.Value(pos))).SetExpansion(1).SetMaxWidth(32).SetAlign(2))
			case arrow.INT32:
				i32 := col.(*array.Int32)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%d", i32.Value(pos))).SetExpansion(1).SetMaxWidth(32).SetAlign(2))
			case arrow.INT64:
				i64 := col.(*array.Int64)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%d", i64.Value(pos))).SetExpansion(1).SetMaxWidth(32).SetAlign(2))
			case arrow.FLOAT16:
				f16 := col.(*array.Float16)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%.2f", f16.Value(pos).Float32())).SetExpansion(1).SetMaxWidth(32).SetAlign(2))
			case arrow.FLOAT32:
				f32 := col.(*array.Float32)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%.2f", f32.Value(pos))).SetExpansion(1).SetMaxWidth(32).SetAlign(2))
			case arrow.FLOAT64:
				f64 := col.(*array.Float64)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%.2f", f64.Value(pos))).SetExpansion(1).SetMaxWidth(32).SetAlign(2))
			case arrow.BOOL:
				b := col.(*array.Boolean)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%t", b.Value(pos))).SetExpansion(1).SetMaxWidth(32).SetAlign(2))
			case arrow.BINARY:
				bi := col.(*array.Binary)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%v", bi.Value(pos))).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.DATE32:
				d32 := col.(*array.Date32)
				results.SetCell(pos+1, i, tview.NewTableCell(d32.Value(pos).ToTime().String()).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.DATE64:
				d64 := col.(*array.Date64)
				results.SetCell(pos+1, i, tview.NewTableCell(d64.Value(pos).ToTime().Local().String()).SetExpansion(2).SetMaxWidth(32).SetAlign(2))

			case arrow.MAP:
				m := col.(*array.Map)
				fmt.Println(m.DataType().ID())
				results.SetCell(pos+1, i, tview.NewTableCell(m.String()).SetExpansion(2).SetMaxWidth(32).SetAlign(0))

			case arrow.DECIMAL128:
				dec := col.(*array.Decimal128)
				scale := int32(2) // Replace 2 with the appropriate scale for your use case
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%f", dec.Value(pos).ToFloat64(scale))).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.INTERVAL_DAY_TIME:
				idt := col.(*array.DayTimeInterval)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%v", idt.Value(pos))).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.TIMESTAMP:
				ts := col.(*array.Timestamp)
				results.SetCell(pos+1, i, tview.NewTableCell(ts.Value(pos).ToTime(arrow.Nanosecond).String()).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			}

		}
	}
	results.ScrollToBeginning()

	app.Draw()
}

func renderStats(statDone chan interface{}, stats *tview.Table, selectedFile string, f *delta_sharing.File) {

	defer close(statDone)
	s, err := f.GetStats()
	if err != nil {
		return
	}
	stats.SetTitle(fmt.Sprintf("Stats file-id:[lightblue]%s,[white] containing [green]%d rows", selectedFile, s.NumRecords))
	var maxValues []string
	var minValues []string
	var nullCount []string
	keys := make([]string, 0, len(s.MaxValues))
	for k := range s.MaxValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, v := range keys {
		switch s.MaxValues[v].(type) {
		case string:
			maxValues = append(maxValues, s.MaxValues[v].(string))
		case float64:
			maxValues = append(maxValues, fmt.Sprintf("%.2f", s.MaxValues[v]))
		case float32:
			maxValues = append(maxValues, fmt.Sprintf("%.2f", s.MaxValues[v]))
		case int16:
			y := s.MaxValues[v].(int16)
			maxValues = append(maxValues, fmt.Sprintf("%d", y))
		case int32:
			maxValues = append(maxValues, fmt.Sprintf("%d", s.MaxValues[v]))
		case int64:
			maxValues = append(maxValues, fmt.Sprintf("%d", s.MaxValues[v]))
		}
		switch s.MinValues[v].(type) {
		case string:
			minValues = append(minValues, s.MinValues[v].(string))
		case float64:
			minValues = append(minValues, fmt.Sprintf("%.2f", s.MinValues[v]))
		case float32:
			minValues = append(minValues, fmt.Sprintf("%.2f", s.MinValues[v]))
		case int16:
			minValues = append(minValues, fmt.Sprintf("%d", s.MinValues[v]))
		case int32:
			minValues = append(minValues, fmt.Sprintf("%d", s.MinValues[v]))
		case int64:
			minValues = append(minValues, fmt.Sprintf("%d", s.MinValues[v]))
		}
		nullCount = append(nullCount, fmt.Sprintf("%.0f", s.NullCount[v]))
	}
	stats.SetCell(0, 0, tview.NewTableCell("Column").SetSelectable(false).SetExpansion(2))
	stats.SetCell(0, 1, tview.NewTableCell("Max").SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(2))
	stats.SetCell(0, 2, tview.NewTableCell("Min").SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(2))
	stats.SetCell(0, 3, tview.NewTableCell("#Nulls").SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(2))
	for row := 0; row < len(keys); row++ {
		for col := 0; col < 4; col++ {
			switch col {
			case 0:
				stats.SetCell(row+1, col, tview.NewTableCell(keys[row]).SetAttributes(tcell.AttrBold).SetExpansion(2))
			case 1:
				stats.SetCell(row+1, col, tview.NewTableCell(maxValues[row]).SetAlign(tview.AlignRight).SetExpansion(2))
			case 2:
				stats.SetCell(row+1, col, tview.NewTableCell(minValues[row]).SetAlign(tview.AlignRight).SetExpansion(2))
			case 3:
				stats.SetCell(row+1, col, tview.NewTableCell(nullCount[row]).SetAlign(tview.AlignRight).SetExpansion(2))
			}
		}
	}
	stats.ScrollToBeginning()
}

func createSchemaTable(schemaDone chan interface{}, sparkSchema *tview.Table, keys []string, meta map[string]retval, tableSelected string) {
	defer close(schemaDone)
	sort.Strings(keys)
	sparkSchema.SetCell(0, 0, tview.NewTableCell("Column").SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(2))
	sparkSchema.SetCell(0, 1, tview.NewTableCell("Type").SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(2))
	sparkSchema.SetCell(0, 2, tview.NewTableCell("Nullable").SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(2))
	sparkSchema.SetCell(0, 3, tview.NewTableCell("Metadata").SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(2))
	for i, v := range keys {
		r := meta[v]
		t := r.Type
		n := r.Nullable
		m := r.Metadata
		sparkSchema.SetCell(i+1, 0, tview.NewTableCell(v).SetAttributes(tcell.AttrBold).SetExpansion(2))
		sparkSchema.SetCell(i+1, 1, tview.NewTableCell(t).SetExpansion(2))
		sparkSchema.SetCell(i+1, 2, tview.NewTableCell(n).SetExpansion(2))
		sparkSchema.SetCell(i+1, 3, tview.NewTableCell(m).SetExpansion(2))
	}
	sparkSchema.SetTitle(fmt.Sprintf("Schema for table: [lightblue]%s", tableSelected))
	sparkSchema.ScrollToBeginning()
}
