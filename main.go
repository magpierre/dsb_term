package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/gdamore/tcell/v2"
	delta_sharing "github.com/magpierre/go_delta_sharing_client"
	"github.com/rivo/tview"
)

func main() {
	//defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	app := tview.NewApplication()
	profile := flag.String("profile", "", "Profile path")
	flag.Parse()
	s := strings.Split(*profile, "#")
	ds, err := delta_sharing.NewSharingClient(context.Background(), s[0])
	if err != nil {
		log.Fatal("Could not open sharing client.")
		os.Exit(-1)
	}

	stats := tview.NewTable()
	results := tview.NewTable()
	inputForm := tview.NewForm()
	sparkSchema := tview.NewTable()
	var pages *tview.Pages
	setSelectionChangedFunction(results, stats, sparkSchema)

	var fileid string
	var tableSelected string
	tab := make(map[string]delta_sharing.Table)
	fls := make(map[string]delta_sharing.File)
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

	/* test start */
	tl := tview.NewFlex().SetDirection(tview.FlexRow)
	et := tview.NewFlex().SetDirection(tview.FlexRow)
	textArea := tview.NewTextArea()
	textArea.SetText("SELECT * FROM table LIMIT 10", true)
	textArea.SetBorder(false).SetTitle("SQL Query")

	et.AddItem(textArea.SetTextStyle(tcell.StyleDefault), 0, 2, true)

	buttons := tview.NewFlex().SetDirection(tview.FlexColumn).AddItem(tview.NewButton("Run Query").SetSelectedFunc(func() {
		et.SetTitle("Running Query...")
	}), 30, 1, false).AddItem(tview.NewButton("Explain Query").SetSelectedFunc(func() {
		et.SetTitle("Explaining Query...")
	}), 30, 1, false)
	et.SetBorder(true)

	tl.AddItem(buttons, 3, 3, false)
	tl.AddItem(et, 0, 1, true)
	/* test end */

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
		var resultDone = make(chan interface{})
		// Get data from Delta Lake table
		go renderResult(resultDone, app, results, ds, tab[tableSelected], fileid)
		var statDone = make(chan interface{})
		go renderStats(statDone, stats, fileid, &f1)
		keys := make([]string, 0, len(meta))
		for k := range meta {
			keys = append(keys, k)
		}
		var schemaDone = make(chan interface{})
		go createSchemaTable(schemaDone, sparkSchema, keys, meta, tableSelected)
		results.SetTitle(fmt.Sprintf("Records in file: [lightblue]%s[white], part of table: [lightblue]%s", fileid, tableSelected))
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
		AddPage("Finder", flex, true, true).
		AddPage("Query", tl, true, false) // Test page

	/* inputform */
	inputForm.SetBorder(true)
	inputForm.SetTitle("Delta Sharing")
	inputForm.AddDropDown("Share", shareStrings, 0, nil)
	inputForm.AddDropDown("Schema", schemaStrings, 0, nil)
	inputForm.AddDropDown("Table", tableStrings, 0, func(option string, optionIndex int) {
		tableSelected = option
		results.Clear()
		f, err := ds.ListFilesInTable(tab[option])
		if err != nil {
			fmt.Println(err)
			return
		}
		schema, err := f.Metadata.GetSparkSchema()
		if err != nil {
			return
		}
		fmt.Println(meta)
		for k := range meta {
			delete(meta, k)
		}
		for _, v := range schema.Fields {
			x := fmt.Sprint(v.Type)
			r := retval{x, fmt.Sprintf("%t", v.Nullable), fmt.Sprintf("%v", v.Metadata)}
			meta[v.Name] = r
		}
		files.Clear()
		for k := range fls {
			delete(fls, k)
		}
		for _, x := range f.AddFiles {
			files.AddItem(x.Id, fmt.Sprintf("Size: %.2f mb (%.2f kb, %.0f bytes)", x.Size/(1<<20), x.Size/(1<<10), x.Size), 0, nil)
			fls[x.Id] = x
		}
		files.SetTitle(fmt.Sprintf("Files in table: [lightgreen]%s[lightgreen]", option))
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
