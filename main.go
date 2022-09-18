package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	delta_sharing "github.com/delta-io/delta_sharing_go"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func renderResult(done chan interface{}, app *tview.Application, flex *tview.Flex, results *tview.Table, client interface{}, table delta_sharing.Table, fileId string) {
	results.Clear()
	results.SetBorders(true)
	defer close(done)

	t, err := delta_sharing.LoadArrowTable(client, table, fileId)
	if err != nil {
		//dialog.SetMessage(fmt.Sprintf("Error: %s", err))
		//pages = pages.AddAndSwitchToPage("error", dialog, true)
		return
	}
	tr := array.NewTableReader(t, 100)
	tr.Retain()
	for i := 0; i < int(t.NumCols()); i++ {
		results.SetCell(0, i, tview.NewTableCell(t.Column(i).Name()).SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(2))
	}

	tr.Next()
	rec := tr.Record()
	for pos := 0; pos < int(rec.NumRows()); pos++ {
		for i, col := range rec.Columns() {
			switch col.DataType().ID() {
			case arrow.STRING:
				a := col.(*array.String)
				results.SetCell(pos+1, i, tview.NewTableCell(a.Value(pos)).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.INT16:
				i16 := col.(*array.Int16)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%d", i16.Value(pos))).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.INT32:
				i32 := col.(*array.Int32)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%d", i32.Value(pos))).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.INT64:
				i64 := col.(*array.Int64)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%d", i64.Value(pos))).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.FLOAT16:
				f16 := col.(*array.Float16)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%.2f", f16.Value(pos))).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.FLOAT32:
				f32 := col.(*array.Float32)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%.2f", f32.Value(pos))).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.FLOAT64:
				f64 := col.(*array.Float64)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%.2f", f64.Value(pos))).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.BOOL:
				b := col.(*array.Boolean)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%t", b.Value(pos))).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.BINARY:
				bi := col.(*array.Binary)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%v", bi.Value(pos))).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.DATE32:
				d32 := col.(*array.Date32)
				results.SetCell(pos+1, i, tview.NewTableCell(d32.Value(pos).ToTime().String()).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.DATE64:
				d64 := col.(*array.Date64)
				results.SetCell(pos+1, i, tview.NewTableCell(d64.Value(pos).ToTime().Local().String()).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
			case arrow.DECIMAL128:
				dec := col.(*array.Decimal128)
				results.SetCell(pos+1, i, tview.NewTableCell(fmt.Sprintf("%f", dec.Value(pos))).SetExpansion(2).SetMaxWidth(32).SetAlign(2))
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
	return
}

func main() {
	app := tview.NewApplication()
	profile := flag.String("profile", "", "Profile path")
	flag.Parse()
	s := strings.Split(*profile, "#")
	ds, err := delta_sharing.NewSharingClient(context.Background(), s[0], "")
	if err != nil {
		log.Fatal("Could not open sharing client.")
		os.Exit(-1)
	}
	stats := tview.NewTable()
	results := tview.NewTable()
	inputForm := tview.NewForm()
	sparkSchema := tview.NewTable()
	var pages *tview.Pages
	results.SetSelectionChangedFunc(func(row, column int) {
		tc := results.GetCell(0, column)
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

	var fileid string
	var tableSelected string
	tab := make(map[string]delta_sharing.Table)
	fls := make(map[string]delta_sharing.File)
	type retval struct {
		Type     string
		Nullable string
		Metadata string
	}
	var shareStrings []string
	var schemaStrings []string
	var tableStrings []string
	meta := make(map[string]retval)
	files := tview.NewList()
	shr, err := ds.ListShares()
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	for _, v := range shr {
		shareStrings = append(shareStrings, v.Name)
		sch, err := ds.ListSchemas(v)
		if err != nil {
			log.Fatal(err)
			os.Exit(-1)
		}
		for _, v2 := range sch {
			schemaStrings = append(schemaStrings, v2.Name)
			tb, err := ds.ListTables(v2)
			if err != nil {
				log.Fatal(err)
				os.Exit(-1)
			}
			for _, v3 := range tb {
				tableStrings = append(tableStrings, v3.Name)
				tab[v3.Name] = v3
			}
		}
	}

	files.ShowSecondaryText(true).SetBorder(true).SetTitle("Files")
	flex := tview.NewFlex().SetDirection(tview.FlexRow)
	formLayout := tview.NewFlex().SetDirection(tview.FlexRow)
	screenLayout := tview.NewFlex().SetDirection(tview.FlexColumn)
	flex2 := tview.NewFlex().SetDirection(tview.FlexRow)
	dataLayout := tview.NewFlex().SetDirection(tview.FlexColumn)

	files.SetSelectedFunc(func(index int, value string, value2 string, runeval rune) {
		stats.Clear()
		fileid = value
		f1 := fls[value]
		s, err := f1.GetStats()
		if err != nil {
			fmt.Println(err)
			return
		}

		var resultDone = make(chan interface{})
		go renderResult(resultDone, app, dataLayout, results, ds, tab[tableSelected], fileid)

		var statDone = make(chan interface{})
		go func(statDone chan interface{}) {
			defer close(statDone)
			stats.SetTitle(fmt.Sprintf("Stats file-id:[lightblue]%s,[white] containing [green]%d rows", value, s.NumRecords))
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
		}(statDone)
		keys2 := make([]string, 0, len(meta))
		for k := range meta {
			keys2 = append(keys2, k)
		}
		var schemaDone = make(chan interface{})
		go func(schemaDone chan interface{}) {
			defer close(schemaDone)
			sort.Strings(keys2)
			sparkSchema.SetCell(0, 0, tview.NewTableCell("Column").SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(2))
			sparkSchema.SetCell(0, 1, tview.NewTableCell("Type").SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(2))
			sparkSchema.SetCell(0, 2, tview.NewTableCell("Nullable").SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(2))
			sparkSchema.SetCell(0, 3, tview.NewTableCell("Metadata").SetSelectable(false).SetAttributes(tcell.AttrBold).SetExpansion(2))
			for i, v := range keys2 {
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
		}(schemaDone)

		results.SetTitle(fmt.Sprintf("Data from file:[lightblue]%s[white] from table:[lightblue]%s", fileid, tableSelected))
	})
	// Create the layout.
	dataLayout.AddItem(results, 0, 2, false)
	flex.AddItem(screenLayout, 0, 1, false)
	flex.AddItem(dataLayout, 0, 1, false)
	flex2.AddItem(sparkSchema, 0, 2, false)
	flex2.AddItem(stats, 0, 2, false)
	formLayout.AddItem(inputForm, 0, 1, true)
	formLayout.AddItem(files, 0, 1, false)
	screenLayout.AddItem(formLayout, 0, 1, true)
	screenLayout.AddItem(flex2, 0, 1, false)
	pages = tview.NewPages().
		AddPage("Finder", flex, true, true)

	/* inputform */
	inputForm.SetBorder(true)
	inputForm.SetTitle("Delta Sharing")
	inputForm.AddDropDown("Share", shareStrings, 0, nil)
	inputForm.AddDropDown("Schema", schemaStrings, 0, nil)
	inputForm.AddDropDown("Table", tableStrings, 0, func(option string, optionIndex int) {
		tableSelected = option
		results.Clear()
		f, _ := ds.ListFilesInTable(tab[option])
		if err != nil {
			fmt.Println(err)
			return
		}
		schema, err := f.Metadata.GetSparkSchema()
		if err != nil {
			return
		}
		for k := range meta {
			delete(meta, k)
		}
		for _, v := range schema.Fields {
			meta[v.Name] = retval{v.Type.(string), fmt.Sprintf("%t", v.Nullable), fmt.Sprintf("%v", v.Metadata)}
		}
		files.Clear()
		for k := range fls {
			delete(fls, k)
		}
		for _, x := range f.AddFiles {
			files.AddItem(x.Id, fmt.Sprintf("Size: %.2f mb (%.2f kb, %.0f bytes)", x.Size/(1<<20), x.Size/(1<<10), x.Size), 0, nil)
			fls[x.Id] = x
		}
		files.SetTitle(fmt.Sprintf("Files in table: [lightblue]%s[lightblue]", option))
		files.SetCurrentItem(0)

	})

	/* results table */
	results.SetBorder(true)
	results.SetBorders(true)
	results.SetSelectable(false, true)
	results.SetBorderPadding(0, 0, 5, 5)

	/* sparkSchema */
	sparkSchema.SetBorders(true)
	sparkSchema.SetBordersColor(tcell.ColorDarkGray)
	sparkSchema.SetFixed(1, 1)
	sparkSchema.SetSelectable(true, false)
	sparkSchema.SetBorder(true)

	/* stats */
	stats.SetBorder(true)
	stats.SetBorders(true)
	stats.SetSelectable(true, false)
	stats.SetBordersColor(tcell.ColorDarkGray)

	app.SetRoot(pages, true)
	if err := app.EnableMouse(true).Run(); err != nil {
		fmt.Printf("Error running application: %s\n", err)
	}
}
