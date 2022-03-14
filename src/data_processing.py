import pandas as pd
pd.set_option('display.width', 400)
pd.set_option('display.max_columns', 10)
from dateutil import rrule

def ingest(e):
    # read the events data into dataframe
    df = pd.read_json(e)
    return df

def top_x_simple_ltv_customers(x, df):
    ### customer details dataframe
    customer_df = df[df['type'] == 'CUSTOMER'].dropna(axis=1).rename({'key': 'customer_id'}, axis=1)
    customer_df = customer_df.reset_index(drop=True)
    print('\n', customer_df)

    ### site visit details dataframe
    site_visit_df = df[df['type'] == 'SITE_VISIT'].dropna(axis=1).rename({'key': 'page_id'}, axis=1)
    site_visit_df = site_visit_df.reset_index(drop=True)
    print('\n', site_visit_df)

    ### image details dataframe
    image_df = df[df['type'] == 'IMAGE'].dropna(axis=1).rename({'key': 'image_id'}, axis=1)
    image_df = image_df.reset_index(drop=True)
    print('\n', image_df)

    ### order details dataframe
    order_df = df[df['type'] == 'ORDER'].dropna(axis=1).rename({'key':'order_id'}, axis=1)
    order_df = order_df.reset_index(drop=True)
    print('\n', order_df)

    ### remove usd from the dollar value and change to type float
    order_df['total_amount'] = order_df['total_amount'].str.replace(' USD', '').astype(float)

    ### total_revenue from each customer
    total_revenue = order_df.groupby('customer_id')['total_amount'].sum().to_frame()
    print('\n', total_revenue)

    ### total visit count from each customer
    total_visits = site_visit_df['customer_id'].value_counts().to_frame().rename({'customer_id':'visit_count'}, axis=1)
    print('\n', total_visits)

    ### calculate lifetime value (ltv_value = 52(a) x t) of a shutterfly customer
    ltv_df = pd.DataFrame(columns=['customer_id', 'last_name', 'total_revenue', 'total_visits', 'ltv'])

    # get unique customer id list
    customer_list = list(customer_df['customer_id'].drop_duplicates())
    print('\n', customer_list)

    for customer in customer_list:
        # get the site visit unique list for every customer
        visit_list = site_visit_df[site_visit_df['customer_id'] == customer]['event_time'].to_list()
        print('site_visit_list:', visit_list)

        # get the number of weeks count based on min and max event time for a customer
        weeks = rrule.rrule(rrule.WEEKLY, dtstart=min(visit_list), until=max(visit_list))
        total_weeks = weeks.count()
        print('number_of_weeks:', total_weeks)

        customer_revenue_per_visit = total_revenue.loc[customer, 'total_amount'] / total_visits.loc[customer, 'visit_count']

        number_of_site_visits_per_week = total_visits.loc[customer, 'visit_count'] / total_weeks

        a = customer_revenue_per_visit * number_of_site_visits_per_week
        t = 10
        ltv = 52 * a * t
        print('ltv value:', ltv)

        # assemble new row to be added to ltv_df
        new_row = pd.Series({'customer_id': customer,
                             'last_name': customer_df[customer_df['customer_id'] == customer]['last_name'].values[0],
                             'total_revenue': total_revenue.loc[customer, 'total_amount'],
                             'total_visits': total_visits.loc[customer, 'visit_count'],
                             'ltv': ltv})

        # sort the ltv_df descending and limit to top x values
        ltv_df = ltv_df.append(new_row, ignore_index=True).sort_values('ltv', ascending=False)

    top_x_ltv = ltv_df.head(x)
    print(top_x_ltv)
    top_x_ltv.to_csv('output/top_x_ltv.csv', sep=',', index=False)

events = 'input/events_mock.txt'
data = ingest(events)
top_x_simple_ltv_customers(3, data)
