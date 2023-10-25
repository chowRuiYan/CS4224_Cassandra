### Preprocess Data

## Get Rid of Null values in data
# Replace null with -1 in order.csv
with open('data_files/order.csv') as f:
    new_order_text=f.read().replace('null', '-1')
with open('data_files/modifiedOrder.csv', "w") as f:
    f.write(new_order_text)

# Replace null with empty in order-line.csv
with open('data_files/order-line.csv') as f:
    new_order_line_text=f.read().replace(',null,', ',,')
with open('data_files/modifiedOrderLine.csv', "w") as f:
    f.write(new_order_line_text)