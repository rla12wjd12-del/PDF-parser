d:\\PDF parser

|   excel\_export.py

|   field\_catalog.py

|   batch\_parse.py

|   test\_validator.py

|   verify\_parsing.py

|   run\_parser.bat

|   main.py

|   requirements.txt

|   pytest.ini

|   PARSING\_ANALYSIS.md

|   ARCH\_REVIEW.md

|   WORKLOG\_table\_parsing\_status\_2026-04-21.md

|   20260420\_PROJECT\_REPORT.md

|   20260420\_PROGRESS\_REPORT.md

|   20260420\_PARSING\_DEEPDIVE\_REPORT.md

|   debug-dcc858.log

|   full\_qualifications.json

|   parse\_final2.txt

|   suspect\_test.txt

|   ...

|

+---parsers

|   |   \_\_init\_\_.py

|   |   page\_1\_parser.py

|   |   page\_2\_parser.py

|   |   page\_3\_parser.py

|   |   page\_summary\_parser.py

|   |   section\_parsers.py

|   |   personal\_info.py

|   |   issuer\_reference.py

|   |   worktype\_classifier.py

|   |   tech\_career\_heuristics.py

|   |   template\_table\_parser.py

|   |   table\_settings.py

|   |   table\_career\_parser.py

|   |   raw\_table\_dump.py

|   |   document\_context.py

|   |   layout\_extractor.py

|   |   quality\_gate.py

|   |   logger.py

|   \\---\_\_pycache\_\_

|       ...

|

+---tools

|   |   \_\_init\_\_.py

|   |   build\_field\_catalog.py

|   +---\_\_pycache\_\_

|   |   ...

|   \\---layout\_debug

|       |   \_\_init\_\_.py

|       |   pdfplumber\_table\_viz.py

|       |   word\_bbox\_viewer\_app.py

|       \\---\_\_pycache\_\_

|           ...

|

+---scripts

|   |   export\_recognized\_tables\_to\_excel.py

|   |   export\_recognized\_tables\_gui.py

|   |   debug\_table\_candidates.py

|   |   export\_raw\_tables\_console.py

|   |   export\_raw\_tables\_uploader\_app.py

|   |   search\_virtual\_borders.py

|   |   export\_tables\_with\_custom\_borders.py

|   |   scan\_xlsx\_formulas.py

|   |   search\_tokens\_in\_xlsx.py

|   |   preview\_xlsx\_table\_block.py

|   |   debug\_header\_override.py

|   |   assert\_tokens\_in\_xlsx.py

|   +---verify

|   |   |   test\_validator.py

|   |   |   verify\_bug\_fixes.py

|   |   |   verify\_parse.py

|   |   |   inspect\_words\_bbox.py

|   |   |   find\_text\_pages.py

|   |   |   inspect\_page\_table\_layout.py

|   |   |   scan\_digit\_density.py

|   |   |   dump\_words\_sample.py

|   |   |   quick\_template\_check.py

|   |   |   find\_name\_contamination\_pages.py

|   |   |   find\_row\_by\_fields.py

|   |   \\---\_\_pycache\_\_

|   |       ...

|   +---viz

|   |   word\_bbox\_viewer\_app.py

|   \\---\_\_pycache\_\_

|       ...

|

+---tests

|   |   test\_duplicate\_tuple\_date\_blocks.py

|   |   test\_tech\_career\_page\_metrics.py

|   |   test\_tilde\_training\_parsers.py

|   |   test\_table\_settings.py

|   \\---\_\_pycache\_\_

|       ...

|

+---data

|   |   field\_catalog.json

|   |   public\_institutions\_tree.json

|   |   public\_institutions\_flat.json

|   |   korea\_regions\_tree.json

|   |   korea\_regions\_flat.json

|   |   building\_categories\_tree.json

|   |   Construction\_categories\_tree.json

|   |   tech\_career\_heuristics.json

|   |   ... (csv/hwp 등)

|

+---excel\_output

|   |   ... (xlsx 다수)

|   \\---raw\_tables

|       +---raw\_tables\_20260421\_195838

|       |   \\---... (xlsx 다수)

|       +---raw\_tables\_20260421\_201553

|       |   \\---... (xlsx 다수)

|       +---raw\_tables\_20260421\_203812

|       |   \\---... (xlsx 다수)

|       \\---... (세트 다수)

|

+---json\_output

|   |   ... (json 다수)

|   \\---json\_output

|       \_tmp\_cha.json

|       \_tmp\_test\_20260416.json

|       \_tmp\_test2\_20260416.json

|

+---reports

|   |   suspect\_test2.txt

|   |   suspect\_excerpts\_20260421\_3.txt

|   \\---validation

|       ... (validation\_report\_\*.txt 다수)

|

+---.claude

|   settings.json

|   settings.local.json

|

+---.pytest\_cache

|   ...

|

\\---.cursor

&#x20;   ...

