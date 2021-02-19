import csv, sys, tempfile, datetime, pytz


def main():
    cb_file_path, cb_pro_file_path, bn_file_path, output_path = get_arguments()
    
    accumulators = {}

    # import and organize the data
    import_coinbase(cb_file_path, accumulators)
    import_coinbase_pro(cb_pro_file_path, accumulators)
    import_binance(bn_file_path, accumulators)

    # make sure the records are all sorted
    for key in accumulators.keys():
        accumulators[key]['Buy'] = sorted(accumulators[key]['Buy'], reverse=True, key = lambda i: i['Date'])
        accumulators[key]['Sell'] = sorted(accumulators[key]['Sell'], reverse=True, key = lambda i: i['Date'])
        accumulators[key]['Earn'] = sorted(accumulators[key]['Earn'], reverse=True, key = lambda i: i['Date'])

    results = process_tax_lines(accumulators)


def get_arguments():
    try:
        cb_file_path = sys.argv[1]
    except:
        cb_file_path = ""
        raise Exception("Argument 1: cb_file_path not supplied. Cannot continue.")
    try:
        cb_pro_file_path = sys.argv[2]
    except:
        cb_pro_file_path = ""
        raise Exception("Argument 1: cb_file_path not supplied. Cannot continue.")
    try:
        bn_file_path = sys.argv[3]
    except:
        bn_file_path = ""
        raise Exception("Argument 1: cb_file_path not supplied. Cannot continue.")
    try:
        output_path = sys.argv[4]
    except:
        output_path = ""
        raise Exception("Argument 2: output_path not supplied. Cannot continue.")

    return cb_file_path, cb_pro_file_path, bn_file_path, output_path

def import_coinbase(file_path, accumulators):
    with open(file_path) as csv_input:
        dialect = csv.Sniffer().sniff(csv_input.read(1024))
        csv_input.seek(0)
        reader = csv.reader(csv_input, dialect)
        accumulators = process_cb_contents(reader, accumulators)

def import_coinbase_pro(file_path, accumulators):
    with open(file_path) as csv_input:
        dialect = csv.Sniffer().sniff(csv_input.read(1024))
        csv_input.seek(0)
        reader = csv.reader(csv_input, dialect)
        accumulators = process_cb_pro_contents(reader, accumulators)

def import_binance(file_path, accumulators):
    with open(file_path) as csv_input:
        dialect = csv.Sniffer().sniff(csv_input.read(1024))
        csv_input.seek(0)
        reader = csv.reader(csv_input, dialect)
        accumulators = process_binance_contents(reader, accumulators)

def process_cb_contents(reader, accumulators):
    skip_count = 0
    for row in reader:
        if skip_count < 1:
            skip_count += 1
            continue
        
        event_type = row[1].lower().strip()
        asset_name = row[2].upper().strip()
        # We don't give a damn about sent coins
        if event_type != 'send':
            # check for new asset type
            if asset_name not in accumulators.keys():
                # prepare accumulator
                accumulators[asset_name] = {
                    'Earn': [],
                    'Buy': [],
                    'Sell': []
                }
            
            if event_type == 'buy': # Not used 2020
                buy_date = localize_utc_string(row[0])
                accumulators[asset_name]['Buy'].append(
                    {'Date': buy_date, 'Amount': float(row[3]), 'Price': float(row[4]), 'Fees': float(row[7]), 'fee_type': 'USD', 'original_fee': float(row[7])}
                )
            elif event_type == 'sell': # Not used 2020
                sell_date = localize_utc_string(row[0])
                accumulators[asset_name]['Sell'].append(
                    {'Date': sell_date, 'Amount': float(row[3]), 'Price': float(row[4]), 'Fees': float(row[7]), 'fee_type': 'USD', 'original_fee': float(row[7])}
                )
            elif event_type == 'coinbase earn':
                earn_date = localize_utc_string(row[0])
                accumulators[asset_name]['Earn'].append(
                    {'Date': earn_date, 'Amount': float(row[3]), 'Price': float(row[4]), 'Fees': float(row[7]), 'fee_type': 'USD', 'original_fee': float(row[7])}
                )
            else:
                print(row)
                raise Exception('Unexpected Event Type in Coinbase CSV! Aborting...')
    
    return accumulators

def process_cb_pro_contents(reader, accumulators):
    skip_count = 0
    for row in reader:
        if skip_count < 1:
            skip_count += 1
            continue
        
        event_type = row[3].lower().strip()
        asset_name = row[2].split('-')[0].upper().strip()
        # check for new asset type
        if asset_name not in accumulators.keys():
            # prepare accumulator
            accumulators[asset_name] = {
                'Earn': [],
                'Buy': [],
                'Sell': []
            }
        
        if event_type == 'buy':
            buy_date = localize_utc_string(row[4].split('.')[0], '%Y-%m-%dT%H:%M:%S')
            accumulators[asset_name]['Buy'].append(
                {'Date': buy_date, 'Amount': float(row[5]), 'Price': float(row[7]), 'Fees': float(row[8]), 'fee_type': 'USD', 'original_fee': float(row[8])}
            )
        elif event_type == 'sell': # Needs updating, not used 2020
            sell_date = localize_utc_string(row[4].split('.')[0], '%Y-%m-%dT%H:%M:%S')
            accumulators[asset_name]['Sell'].append(
                {'Date': sell_date, 'Amount': float(row[5]), 'Price': float(row[7]), 'Fees': float(row[8]), 'fee_type': 'USD', 'original_fee': float(row[8])}
            )
        else:
            print(row)
            raise Exception('Unexpected Event Type in Coinbase Pro CSV! Aborting...')
    return accumulators

def process_binance_contents(reader, accumulators):
    binance_fees_bnb = .075 / 100
    skip_count = 0
    for row in reader:
        if skip_count < 1:
            skip_count += 1
            continue
        
        event_type = row[2].lower().strip()
        asset_name = row[1][:-3].upper().strip()  # Remove USD
        fee_type = row[7].upper().strip()

        # check for new asset type
        if asset_name not in accumulators.keys():
            # prepare accumulator
            accumulators[asset_name] = {
                'Earn': [],
                'Buy': [],
                'Sell': []
            }

        buy_date = localize_utc_string(row[0], '%Y-%m-%d %H:%M:%S')
        fee_calc = float(row[6])
        if fee_type == 'BNB':
            fee_calc = float(row[4]) * float(row[3]) * binance_fees_bnb
        elif fee_type == 'USD':
            pass # already in USD
        else: # paid in whatever the buy was (ETH BTC etc.) this value needs
            fee_calc = fee_calc * float(row[3])
        
        if event_type == 'buy':
            accumulators[asset_name]['Buy'].append(
                {'Date': buy_date, 'Amount': float(row[4]), 'Price': float(row[3]), 'Fees': fee_calc, 'fee_type': fee_type, 'original_fee': float(row[6])}
            )
        elif event_type == 'sell':
            accumulators[asset_name]['Sell'].append(
                {'Date': buy_date, 'Amount': float(row[4]), 'Price': float(row[3]), 'Fees': fee_calc, 'fee_type': fee_type, 'original_fee': float(row[6])}
            )
        else:
            print(row)
            raise Exception('Unexpected Event Type in Binance CSV! Aborting...')

# {'property': string, 'aquired': date, 'sold': date, 'proceeds': float, 'cost_basis': float, 'gain_loss': float}
def process_tax_lines(accumulators):
    earned_income = []
    taxable_transactions = []
    
    for key in accumulators.keys():
        # first deal with "additional" income i.e. coinbse earn
        if len(accumulators[key]['Earn']) != 0:
            for record in accumulators[key]['Earn']:
                earned_income.append({'coin': key, 'value_when_earned': record['Price'], 'taxable_amount': record['Amount'] * record['Price']})
                accumulators[key]['Buy'].append(record)
            # Resort after adding new records
            accumulators[key]['Buy'] = sorted(accumulators[key]['Buy'], reversed = True, key=lambda i: i['Date'])
        # nexy handle sell records
        for sell in accumulators[key]['Sell']:
            # each sell must match with one or more buys
            while True:
                buy = accumulators[key]['Buy'].pop()
                if sell['Amount'] > buy['Amount']:
                    pass
                elif sell['Amount'] < buy['Amount']:
                    pass
                else:
                    pass

def localize_utc_string(date_string, format_string='%Y-%m-%dT%H:%M:%SZ'):
    utc_zone = pytz.utc.localize(datetime.datetime.strptime(date_string, format_string))
    return utc_zone.astimezone(pytz.timezone('US/Pacific'))

if __name__ == '__main__':
    main()
