import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import unquote


class Tourism:
    def __init__(self, data_dir="data", checkpoint_path="checkpoint.txt"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

        self.base_url = "https://admin.taiwan.net.tw"
        self.url = "https://admin.taiwan.net.tw/businessinfo/FilePage?a=12603"

        self.checkpoint_path = checkpoint_path
        self.checkpoint = self.get_checkpoint()

    def get_checkpoint(self):

        # 檢查checkpoint檔案是否存在
        if not os.path.exists(self.checkpoint_path):

            # 如果檔案不存在，創建並寫入初始內容
            with open(self.checkpoint_path, "w", encoding="utf-8") as f:
                f.write("113-12-01")

            print(f"Checkpoint file created with default content: 113-01-01")

        with open(self.checkpoint_path, "r", encoding="utf-8") as f:
            return f.read()

    def update_checkpoint(self, checkpoint):
        self.checkpoint = checkpoint

        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            f.write(checkpoint)

    def get_xlsx_links(self):

        # 取得html
        response = requests.get(self.url)
        html = response.text

        # 取得table內容
        soup = BeautifulSoup(html, "html.parser")
        body = soup.body
        tbody = body.find("tbody")

        links = []

        # 逐行尋找
        for tr in tbody.find_all("tr"):
            tds = tr.find_all("td")

            # 找checkpoint以後更新的資料
            update_date = tds[-1].text

            if update_date < self.checkpoint:
                continue

            # 取得xlsx檔連結
            a = tds[2].find("a", title=lambda t: t and "檔案格式：XLS" in t)
            links.append(self.base_url + a.get("href"))

        return links

    def download_xlsx(self, xlsx_links):
        for xlsx_link in xlsx_links:
            print(f"URL: {xlsx_link}")
            response = requests.get(xlsx_link)

            if response.status_code == 200:

                # 提取 Content-Disposition 標頭中的檔名
                content_disposition = response.headers.get("Content-Disposition")
                filename = content_disposition.split("filename=")[-1]

                # 解碼檔案名稱
                xlsx_name = unquote(filename)

                # 建立檔案路徑
                xlsx_path = os.path.join(self.data_dir, xlsx_name)

                # 存檔
                with open(xlsx_path, "wb") as f:
                    f.write(response.content)
                    print(f"Saved {xlsx_path}.")

            else:
                raise Exception(
                    f"Failed to download file. Status code: {response.status_code}"
                )


if __name__ == "__main__":
    tourism = Tourism(data_dir="data", checkpoint_path="checkpoint.txt")
    xlsx_links = tourism.get_xlsx_links()
    tourism.download_xlsx(xlsx_links)
