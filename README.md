# finalyca_lib

Python Modules containing all the important functionalities for Finalyca

Data Store: Data Collection
SQLTable: Data Table
DataSchema: Data Table Definition
DataField: Data Field Definition


# Screener Query Builder

## Filter
| Type | = | != | > | < | >= | <= | has | in | between |
| - | - | - | - | - | - | - | - | - | - |
| BOOL | &check; | &check; | | | | | | | |
| TEXT | &check; | &check; | | | | | &check; | &check; |  |
| REF | &check; | &check; | | | | | | &check; | |
| INT | &check; | &check; | &check; | &check; | &check; | &check; | | &check; | &check; |
| DECIMAL | &check; | &check; | &check; | &check; | &check; | &check; | | &check; | &check; |
| TS | &check; | &check; | &check; | &check; | &check; | &check; | | &check; | &check; |
| DATE | &check; | &check; | &check; | &check; | &check; | &check; | | &check; | &check; |

> Rest of the field types are not supported.

## Aggregation
| Type | Count | Sum | Max | Min | Average | Std Deviation |
| -  | - | - | - | - | - | - |
| BOOL | &check; | | | | |  |
| TEXT | &check; | | | | | |
| REF | &check; | | | | | | 
| INT | &check; | &check; | &check; | &check; | &check; | &check;|
| DECIMAL | &check; | &check; | &check; | &check; | &check; | &check;|
| TS | &check; | | &check; | &check; | | |
| DATE | &check; | |  &check; | &check;  | | |

> Rest of the field types are not supported.


# PDF Scrapping
Broadly pdf scraping libraries can be separated into 2 sections.
1. low level libraries that allows extracting text with (x0, y0, x1, y1) rects
2. high level libraries that uses low level libraries and gives easy to access function e.g. extract_table.

we are using pdf plumber for our use case.

## Installing pdf plumber
``` shell
pip install pdfplumber
```

## Visual debugging

Install ImageMagic and GhostScript.

On Windows, download the applications [ImageMagic](https://imagemagick.org/script/download.php#windows) and [GhostScript](https://ghostscript.com/releases/gsdnld.html)

On Ubuntu, 
```
sudo apt-get install libmagickwand-dev
sudo apt-get install ghostscript
```

> Fix for Image magic
Add the following line in `/etc/ImageMagick-7/policy.xml` just before `</policymap>`.
``` xml
<policy domain="coder" rights="read | write" pattern="PDF" />
```

## Reference:
More PDF Libraries:
https://cbrunet.net/python-poppler/usage.html#working-with-pages

