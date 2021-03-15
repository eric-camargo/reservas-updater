def merge_days(sheet_id):

    days_merging = {
                "requests": [
                    {
                        "mergeCells": {
                            "range": {  # In this sample script, all cells of "A1:C3" of "Sheet1" are merged.
                                "sheetId": sheet_id,
                                "startRowIndex": 1,
                                "endRowIndex": 2,
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
                                "startRowIndex": 1,
                                "endRowIndex": 2,
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
                                "startRowIndex": 1,
                                "endRowIndex": 2,
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
                                "startRowIndex": 1,
                                "endRowIndex": 2,
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
                                "startRowIndex": 1,
                                "endRowIndex": 2,
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
                                "startRowIndex": 1,
                                "endRowIndex": 2,
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
                                "startRowIndex": 1,
                                "endRowIndex": 2,
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
                                "startRowIndex": 1,
                                "endRowIndex": 2,
                                "startColumnIndex": 15,
                                "endColumnIndex": 17
                            },
                            "mergeType": "MERGE_ROWS"
                        }
                    }
                ]
            }
    return days_merging