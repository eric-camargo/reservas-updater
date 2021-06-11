def rename_sheet(src_sheet_id, new_name):
    body = {
        "requests": [
            {
                "updateSheetProperties":
                    {
                    "properties":
                        {
                        "sheetId": src_sheet_id,
                        "title": new_name
                    },
                    "fields": "title",
                }
            }
        ]
    }
    return body