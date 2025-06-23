# Go-Data-Tool
Universal CLI utility for working with CSV files, integrating with external APIs, backup and logging

This project is a trial run in creating a full-fledged multitasking utility in Golang.

## 🚀 Features
- 📃 Parses CSV files and provides an interface for filtering and aggregation
- 🚚 Send data from a CSV file to a third-party REST API
- 🔒 Creates backup copies of files
- 📝 Maintains logging of actions
- 🚧 Covered with tests for major components

## 📦 Technologies
- Go
- [Cobra](https://github.com/spf13/cobra)

## 🛠️ Installation
```bash
git clone https://github.com/ArtemDaemon/go-data-tool.git
cd go-data-tool
```

## 🏃 Running
The utility has the following commands:
### CSV-parsing: `go-data-tool parse`
Parsing, processing and outputting CSV data. Flags:
- `input` - file address for processing
- `output` - output file address
- `filter` - set of filters in the format "column operation value";
can be passed in by separating them with commas or by reusing the flag;
values for comparison by greater than and less than operations must be numeric;
possible operations: `=, !=, >, >=, <, <=`

## 🗒️ License
MIT License - use it freely, improve it, share it :party_popper:
