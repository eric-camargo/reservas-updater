def merge_days(sheet_id):
    """Código do Google API para fazer Merge das células do Header"""
    START_ROW = 1
    END_ROW = 3

    days_merging = {
                "requests": [
                    {
                        "mergeCells": {
                            "range": {  # In this sample script, all cells of "A1:C3" of "Sheet1" are merged.
                                "sheetId": sheet_id,
                                "startRowIndex": START_ROW,
                                "endRowIndex": END_ROW,
                                "startColumnIndex": 1,
                                "endColumnIndex": 3
                            },
                            "mergeType": "MERGE_ROWS"
                        }
                    },
                    {
                        "mergeCells": {
                            "range": {  # In this sample script, all cells of "A1:C3" of "Sheet1" are merged.
                                "sheetId": sheet_id,
                                "startRowIndex": START_ROW,
                                "endRowIndex": END_ROW,
                                "startColumnIndex": 3,
                                "endColumnIndex": 5
                            },
                            "mergeType": "MERGE_ROWS"
                        }
                    },
                    {
                        "mergeCells": {
                            "range": {  # In this sample script, all cells of "A1:C3" of "Sheet1" are merged.
                                "sheetId": sheet_id,
                                "startRowIndex": START_ROW,
                                "endRowIndex": END_ROW,
                                "startColumnIndex": 5,
                                "endColumnIndex": 7
                            },
                            "mergeType": "MERGE_ROWS"
                        }
                    },
                    {
                        "mergeCells": {
                            "range": {  # In this sample script, all cells of "A1:C3" of "Sheet1" are merged.
                                "sheetId": sheet_id,
                                "startRowIndex": START_ROW,
                                "endRowIndex": END_ROW,
                                "startColumnIndex": 7,
                                "endColumnIndex": 9
                            },
                            "mergeType": "MERGE_ROWS"
                        }
                    },
                    {
                        "mergeCells": {
                            "range": {  # In this sample script, all cells of "A1:C3" of "Sheet1" are merged.
                                "sheetId": sheet_id,
                                "startRowIndex": START_ROW,
                                "endRowIndex": END_ROW,
                                "startColumnIndex": 9,
                                "endColumnIndex": 11
                            },
                            "mergeType": "MERGE_ROWS"
                        }
                    },
                    {
                        "mergeCells": {
                            "range": {  # In this sample script, all cells of "A1:C3" of "Sheet1" are merged.
                                "sheetId": sheet_id,
                                "startRowIndex": START_ROW,
                                "endRowIndex": END_ROW,
                                "startColumnIndex": 11,
                                "endColumnIndex": 13
                            },
                            "mergeType": "MERGE_ROWS"
                        }
                    },
                    {
                        "mergeCells": {
                            "range": {  # In this sample script, all cells of "A1:C3" of "Sheet1" are merged.
                                "sheetId": sheet_id,
                                "startRowIndex": START_ROW,
                                "endRowIndex": END_ROW,
                                "startColumnIndex": 13,
                                "endColumnIndex": 15
                            },
                            "mergeType": "MERGE_ROWS"
                        }
                    },
                    {
                        "mergeCells": {
                            "range": {  # In this sample script, all cells of "A1:C3" of "Sheet1" are merged.
                                "sheetId": sheet_id,
                                "startRowIndex": START_ROW,
                                "endRowIndex": END_ROW,
                                "startColumnIndex": 15,
                                "endColumnIndex": 17
                            },
                            "mergeType": "MERGE_ROWS"
                        }
                    }
                ]
            }
    return days_merging