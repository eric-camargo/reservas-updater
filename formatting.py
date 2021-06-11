def formatting_sheet(sheet_id):
    formatting = {
        "requests": [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": 'COLUMNS',
                        "startIndex": 0,
                        "endIndex": 1,
                    },
                    "properties": {
                        "hiddenByUser": True,
                    },
                    "fields": 'hiddenByUser',
                }
            }
        ]
    }
    return formatting


