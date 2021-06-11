def copy_format(src_sheet_id, dest_sheet_id):
    body = {
        "requests": [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": src_sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1000,
                        "startColumnIndex": 0,
                        "endColumnIndex": 35
                    },
                    "destination": {
                        "sheetId": dest_sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1000,
                        "startColumnIndex": 0,
                        "endColumnIndex": 35
                    },
                    "pasteType": "PASTE_FORMAT"
                }
            }
        ]
    }
    return body
