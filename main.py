from notion_client import Client
import json
from urllib.request import urlretrieve
from pprint import pprint
from multiprocessing import Pool
from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.

notion_secret_key = os.environ.get('notion_secret_key')
notion_database_id = os.environ.get('notion_database_id')

notion = Client(auth=notion_secret_key)

notion_api_database = [notion_database_id, ""]
file_array = []


def multiply(input_tuple):
    print(input_tuple[0])
    print(input_tuple[1])
    try:
        # urlretrieve(input_tuple[0], r'D:\\test_book\\' + input_tuple[1])
        urlretrieve(input_tuple[0], './books/' + input_tuple[1])
    except:
        print("Error")


def main():
    while True:
        if notion_api_database[0] != "":
            json_data = response_search(notion_api_database[1])
            notion_file_array(json_data)

            if json_data['has_more'] == True:
                print(json_data['next_cursor'])
                notion_api_database[1] = json_data['next_cursor']
            else:
                notion_api_database[1] = ""

                pprint(file_array)
                print(len(file_array))
                p.map(multiply, file_array)  # 더 이상 읽을 목록이 없다면 10개씩 동시에 다운로드 시작
                break
        else:
            break


def response_search(start_cursor):
    if start_cursor == "":
        response = notion.databases.query(database_id=notion_api_database[0])
    else:
        response = notion.databases.query(database_id=notion_api_database[0], start_cursor=start_cursor)
    json_val = json.dumps(response)
    json_data = json.loads(json_val)
    return json_data


def notion_file_array(json_data):
    for results in json_data['results']:

        if len(results["properties"][list((results["properties"]).keys())[0]]['files']) > 0:
            file_name = results["properties"][list((results["properties"]).keys())[0]]['files'][0]['name']
            file_url = results["properties"][list((results["properties"]).keys())[0]]['files'][0]['file']['url']
            sub_file_array = [file_url, file_name]
            file_array.append(sub_file_array)


if __name__ == "__main__":
    p = Pool(processes=10)  # 10개의 프로세스를 사용합니다.
    main()
