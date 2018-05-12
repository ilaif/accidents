from os import path
import json
import pandas as pd
import multiprocessing


def print_t(msg):
    print('%s: %s' % (multiprocessing.current_process().name, msg))


def basic_enrichment(df, helper_data_path):
    # This is a child notebook that should run inside a parent notebook

    print_t('Enriching input dataframe...')

    print_t('Adding missing values as features enrichment...')
    df['has_description'] = df['description'].notna()
    df['has_price'] = df['price'].notna()
    df['has_params'] = df['param_1'].notna()
    df['has_image'] = df['image'].notna()

    print_t('Adding datetime enrichment...')
    df['year'] = df['activation_date'].apply(lambda d: d.year)
    df['month'] = df['activation_date'].apply(lambda d: d.month)
    df['day'] = df['activation_date'].apply(lambda d: d.day)

    print_t('Adding geocoding enrichment...')
    cities_geo = json.load(open(path.join(helper_data_path, 'cities_geo.json'), 'r'))
    cities_geo_df = pd.DataFrame(cities_geo).transpose().reset_index()
    cities_geo_df.columns = ['city', 'lat', 'lng']
    df = df.merge(cities_geo_df, on='city')

    print_t('Adding basic language enrichment...')
    df['description_len'] = df['description'].apply(lambda t: t if pd.isnull(t) else len(t))
    df['title_len'] = df['title'].apply(lambda t: t if pd.isnull(t) else len(t))

    def merge_params(row):
        sentence = ""
        for att in ["param_1", "param_2", "param_3"]:
            if not pd.isnull(row[att]):
                sentence += row[att] + " "
        return sentence

    def count_words_safe(s):
        if pd.isnull(s):
            return 0
        return len(s.split())

    df['title_word_count'] = df['title'].apply(lambda t: count_words_safe(t))
    df['description_word_count'] = df['description'].apply(lambda d: count_words_safe(d))
    df['params_total_word_count'] = df.apply(lambda row: count_words_safe(merge_params(row)), axis=1)

    def count_capitals_ratio_safe(s):
        if pd.isnull(s) or len(s) == 0:
            return 0
        s = s.replace(" ", "")
        return len([l for l in s if l.isupper()]) / len(s)

    df['title_capital_letters_ratio'] = df['title'].apply(lambda t: count_capitals_ratio_safe(t))
    df['description_capital_letters_ratio'] = df['description'].apply(lambda d: count_capitals_ratio_safe(d))

    print_t('Adding translation enrichment...')
    for col_name in ['parent_category_name', 'category_name', 'param_1', 'param_2', 'param_3']:
        trans_dict = json.load(open('helper_data/' + col_name + '_en.json', 'r'))
        df[col_name + '_en'] = df[col_name].apply(lambda t: trans_dict[t] if not pd.isnull(t) else t)

    print_t('Adding user related enrichment...')
    user_ads = df['user_id'].value_counts()
    df['user_ads_count'] = df['user_id'].apply(lambda user_id: user_ads[user_id])

    print_t('Finished enriching.')
    return df
