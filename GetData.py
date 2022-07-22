# ライブラリ
import os
from tqdm import tqdm
from collections import OrderedDict
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd



def GetWebPageTable(year, place, number, day, race):
    """
    レースの結果のWebページの表を取得

    Parameters
    ----------
    year : int　西暦何年
    place : int 競馬場のID
                (1:札幌、2:函館、3:福島、4:新潟、5:東京、
                6:中山、7:中京、8:京都、9:阪神、10:小倉)
    number : int 第何回のレース
    day : int 何日目
    race : int 何レース目
    Returns
    -------
    race_table :html形式　レースの結果のWebページの表
    race_page :html形式　レースの結果のWebページデータ
    race_ID : string レースID
    """

    # URLを作成
    base_url = 'https://db.sp.netkeiba.com/race/'  # URL先頭の固定部分
    # zfill関数を使って「3」→「03」のように2ケタ表示に変換
    race_ID = str(year) + str(place).zfill(2) + str(number).zfill(2) + str(day).zfill(2) + str(race).zfill(2)
    race_url = base_url + race_ID

    # サイトのデータ取得
    race_html = requests.get(race_url)  # リクエスト
    race_html.encoding = race_html.apparent_encoding  # 日本語の文字化け対策
    race_page = BeautifulSoup(race_html.text, 'lxml')  # Webページのデータ
    race_table = race_page.find(class_="table_slide_body ResultsByRaceDetail")  # 表を取得
    time.sleep(1)  # リクエスト送信時間確保

    return race_table, race_page, race_ID


def CommonData2List(race_page, race_ID, place, number, day, race):
    """
    Webページのレース会場のデータをリストに変換

    Parameters
    ----------
    race_page :html形式　レースの結果のWebページデータ
    race_ID : string レースID
    place : int 競馬場のID
                (1:札幌、2:函館、3:福島、4:新潟、5:東京、
                6:中山、7:中京、8:京都、9:阪神、10:小倉)
    number : int 第何回のレース
    day : int 何日目
    race : int 何レース目
    Returns
    -------
    race_data :レースの会場のデータを格納したリスト
        0:レースID、1:年, 2:月, 3:日, 4:曜日, 5:場所, 6:回数,
        7:日目, 8:レース目, 9:レース名, 10:天気, 11:馬場状態,
        12:レースの条件, 13:芝　or ダート, 14:距離, 15:右回り　or 左回り
    """
    # 場所辞書
    place_id2name = ["札幌", "函館", "福島", "新潟", "東京", "中山", "中京", "京都", "阪神", "小倉"]
    try:
        # レース会場の情報を取得
        race_data = [race_ID]
        race_data += race_page.find_all(class_="Race_Date")[0].contents[0].string.strip().split('/')  # 日付[年,月,日]
        race_data.append(race_page.find_all(class_="Race_Date")[0].contents[1].string.strip().replace('(', '').replace(')', ''))  # 曜日
        race_data.append(place_id2name[place-1])  # 場所
        race_data.append(number)  # 回数
        race_data.append(day)  # 日目
        race_data.append(race)  # レース目
        race_data.append(race_page.find_all(class_="RaceName_main")[0].string)  # レース名
        race_data.append(race_page.find_all(class_="RaceData")[0].contents[5].contents[0])  # 天気
        race_data.append(race_page.find_all(class_="RaceData")[0].contents[7].string)  # 馬場状態
        race_data.append(race_page.find_all(class_="RaceHeader_Value_Others")[0].contents[3].string)  # レースの条件
        race_data.append(race_page.find_all(class_="RaceData")[0].contents[3].string[0])  # 芝　or ダート
        race_data.append(race_page.find_all(class_="RaceData")[0].contents[3].string[1:6].replace('m', ''))  # 距離
        race_data.append(race_page.find_all(class_="RaceData")[0].contents[3].string[6:].replace('(', '').replace(')',''))  # 右回り　or 左回り
    except:  # 情報が取得できなかった時
        race_data = []
    return race_data

def WebData2Pandas(race_df, race_table, race_data):
    """
    Webページのデータ(会場情報・レース結果)をpandasに変換

    Parameters
    ----------
    race_df: pandas レース結果の格納先
    race_table :html形式　レースの結果のWebページの表
    race_data :レースの会場のデータを格納したリスト
        0:レースID、1:年, 2:月, 3:日, 4:曜日, 5:場所, 6:回数,
        7:日目, 8:レース目, 9:レース名, 10:天気, 11:馬場状態,
        12:レースの条件, 13:芝　or ダート, 14:距離, 15:右回り　or 左回り
    Returns
    -------
    race_df :レースのデータ(会場と結果)を格納したpandas
    flag :　データを取得できたか
    """
    pre_df = race_df.copy()
    horse_num = len(race_table.find_all('tr')[1:])  # 出走馬数
    flag = True
    try:
        for tr in race_table.find_all('tr')[1:]:  # trタグごとに処理。最初のtrタグは表のカラム名なのでスキップ
            # 出場馬ごとの情報の抽出
            horse_data = [horse_num]
            for idx, td in enumerate(tr.find_all('td')):
                if idx == 4:  # 性別と年齢の格納時
                    horse_data.append(td.string[0])
                    horse_data.append(td.string[1:])
                elif idx == 14:  # 体重と体重の増減
                    horse_data.append(td.string[0:3])
                    horse_data.append(td.string[3:].replace('(', '').replace(')','').replace('+',''))
                elif idx == 15 or idx == 16:  # 有料情報（調教タイム、コメント）
                    continue
                else:
                    if None == td.string:
                        if (idx == 8) or (idx == 20):  # 着差 or 賞金
                            horse_data.append(0)
                        else:
                            horse_data.append('NoData')
                    else:
                        if (td.string == '') or (td.string.replace('\n', '') == ''):
                            if (idx == 8) or (idx == 20):  # 着差 or 賞金
                                horse_data.append(0)
                            else:
                                horse_data.append('NoData')
                        else:
                            horse_data.append(td.string.replace('\n', ''))
            race_df = pd.concat([race_df, pd.DataFrame(race_data+horse_data).T])
    except:  # 情報が取得できなかった時
        race_df = pre_df
        flag = False
    return race_df, flag

if __name__ == "__main__":
    fileName = 'HorseRacing'  # ファイル名の設定
    range_year = list(range(2010, 2023))  # 年の設定
    range_place = list(range(1, 11))  # 場所の設定
    range_number = list(range(1, 12))  # 回数の設定
    range_day = list(range(1, 12))  # 日数の設定
    range_race = list(range(1, 13))  # レース数の設定
    os.makedirs('Data', exist_ok=True)
    with tqdm(range_year) as pbar:
        for year in pbar:
            pbar.set_description(f'[{year}年　({range_year[0]}年→{range_year[-1]}年)]')
            breakList = []  # WEBサイトがないレースIDを格納するリスト
            ngList = []  # データをうまく取得できなかったレースIDを格納するリスト
            race_df = pd.DataFrame()  # 取得したデータ
            for place in range_place:
                for number in range_number:
                    for day in range_day:
                        for race in range_race:
                            try:
                                pbar.set_postfix(OrderedDict(place=place, number=number, day=day, race=race))
                                race_table, race_page, race_ID = GetWebPageTable(year, place, number, day, race)
                                if (race_table == []) | (race_table == None):  # race_tableが上手く取得できていない場合、その後の処理はスキップ
                                    breakList.append(race_ID)
                                    break
                                race_data = CommonData2List(race_page, race_ID, place, number, day, race)
                                if race_data == []:  # race_dataが上手く取得できていない場合、その後の処理はスキップ
                                    ngList.append(race_ID)
                                    break
                                race_df, getFlag = WebData2Pandas(race_df, race_table, race_data)
                                if not getFlag:  # race_dfが上手く取得できていない場合、その後の処理はスキップ
                                    ngList.append(race_ID)
                                    break
                            except:
                                ngList.append(race_ID)
                                continue
