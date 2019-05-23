VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "Sheet1"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = True
Attribute VB_Exposed = True
Sub ticker()
'initialize variables
    Dim j As Integer 'row counter for sorted ticker table
    Dim i As Double
    Dim price_open As Double
    Dim price_close As Double
    Dim yearly_change As Double
    Dim percent_change As Double
    Dim max_change As Double
    Dim min_change As Double
    Dim max_vol As Double

For Each ws In Worksheets 'start worksheet loop
        

        
    LastRow = ws.Cells(Rows.Count, 1).End(xlUp).Row 'Find last row in worksheet
    
    'Set variables to initial conditions for every new worksheet
    j = 2 'since row 1 will be used for headers, start j at 2
    max_change = 0
    min_change = 0
    max_vol = 0
    
    'Write headers
    ws.Range("I1") = "Ticker"
    ws.Range("J1") = "Yearly Change"
    ws.Range("K1") = "Percent Change"
    ws.Range("L1") = "Total Stock Volume"



    For i = 2 To LastRow ' loop through to find all the different tickers
 
         If i = 2 Then 'for first ticker
            vol = ws.Cells(i, 7).Value 'store first volume value
            price_open = ws.Cells(2, 3) 'capture opening price
        
        ElseIf i = LastRow Then 'condition for last ticker
 
            vol = vol + ws.Cells(i, 7).Value 'volume incrementing
            
            ws.Cells(j, 12).Value = vol 'write volume in appropriate cell
            ws.Cells(j, 9).Value = ws.Cells(i, 1).Value 'write ticker name in appropriate cell
            price_close = ws.Cells(i, 6)  'set the closing ticker price
            
            yearly_change = price_close - price_open
            If yearly_change <> 0 Then 'if statement to check if there was a yearly change
        
                percent_change = ((price_close - price_open) / price_open) 'calculate, write and format the yearly change
                ws.Cells(j, 10) = yearly_change
                ws.Cells(j, 11) = percent_change
                ws.Cells(j, 11).NumberFormat = "0.00%"
            
            Else
                ws.Cells(j, 10).Value = 0
                ws.Cells(j, 11).Value = 0
            End If
            
            If ws.Cells(j, 10).Value < 0 Then 'change background color based on yearly change
        
                    ws.Cells(j, 10).Interior.ColorIndex = 3
        
                 ElseIf ws.Cells(j, 10).Value > 0 Then
        
                    ws.Cells(j, 10).Interior.ColorIndex = 4
        
                 End If
 
 
 
        ElseIf ws.Cells(i, 1).Value <> ws.Cells(i - 1, 1).Value Then 'statement to find different tickers that are neither the first nor the last of the worksheet
 
            ws.Cells(j, 12).Value = vol 'write the total volume in appropriate cell
            ws.Cells(j, 9).Value = ws.Cells(i - 1, 1).Value 'write ticker name in correct cell
 
            price_close = ws.Cells(i - 1, 6) 'set the closing price for ticker
            yearly_change = price_close - price_open
                If yearly_change <> 0 Then 'check if there was a yearly change or not
                
                    percent_change = ((price_close - price_open) / price_open) 'calculate, write and format the yearly change
                    ws.Cells(j, 10).Value = yearly_change
                    ws.Cells(j, 11).Value = percent_change
                    ws.Cells(j, 11).NumberFormat = "0.00%"
                    
                
                Else
                    ws.Cells(j, 10).Value = 0
                    ws.Cells(j, 11).Value = 0
                End If
                
        
                 If ws.Cells(j, 10).Value < 0 Then 'change background color
        
                    ws.Cells(j, 10).Interior.ColorIndex = 3
        
                 ElseIf ws.Cells(j, 10).Value > 0 Then
        
                    ws.Cells(j, 10).Interior.ColorIndex = 4
        
                 End If
        
            'reset variables for next ticker and increment j by 1
            price_open = ws.Cells(i, 3).Value
            vol = ws.Cells(i, 7).Value
            j = j + 1
 
        Else
    
            vol = vol + ws.Cells(i, 7).Value 'volume incrementing if none of the above conditions are met
 
        End If
        
        If price_open = 0 And ws.Cells(i, 3).Value > 0 Then 'if statement to ensure actual opening price is captured (non-zero value)
            price_open = ws.Cells(i, 3).Value
        End If
 
    Next i
    'write the rest of the headers
    ws.Range("P1") = "Ticker"
    ws.Range("Q1") = "Value"
   ws.Range("O2") = "Greatest % Increase"
    ws.Range("O3") = "Greatest % Decrease"
    ws.Range("O4") = "Greatest Total Volume"
        
        For i = 2 To j 'loop to find max volume, and the greatest % Increase and Decrease, and format cells accordingly
 
             If ws.Cells(i, 12).Value > max_vol Then
                max_vol = ws.Cells(i, 12).Value
                ws.Range("Q4") = max_vol
                ws.Range("P4") = ws.Cells(i, 9).Value
            End If
    
            If ws.Cells(i, 11).Value > max_change Then
                max_change = ws.Cells(i, 11)
                ws.Range("Q2") = max_change
                ws.Range("Q2").NumberFormat = "0.00%"
                ws.Range("P2") = ws.Cells(i, 9).Value
            
            ElseIf ws.Cells(i, 11) < min_change Then
                min_change = ws.Cells(i, 11)
                ws.Range("Q3") = min_change
                ws.Range("Q3").NumberFormat = "0.00%"
                ws.Range("P3") = ws.Cells(i, 9).Value
                
            End If
    
        Next i
    
Next ws

End Sub






