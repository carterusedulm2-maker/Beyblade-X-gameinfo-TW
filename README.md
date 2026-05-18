# 戰鬥陀螺比賽 台灣 2026

台灣戰鬥陀螺 X G3 比賽資訊整合網站，自動從官方 Google Sheets 抓取資料。

## 功能

- 📋 全台比賽列表（Funbox 門市 + B4 合作據點）
- 🔍 按縣市、報名方式、賽制篩選
- ↕️ 按日期、名額排序
- 📱 手機友善 RWD
- ⏰ 每日自動檢查更新（GitHub Actions）
- 🧭 自動從索引頁與搜尋結果發現每兩個月新增的 G3/B4 Google Sheets
- 🧹 自動移除已過期賽事，列表只保留今天與未來場次

## 資料來源

- [戰鬥陀螺 TW 官方](https://www.facebook.com/Beyblade2016/) 公布的 Google Sheets
- [HackMD 索引頁](https://hackmd.io/@liangyutw/beyblade-important-record)

## 本地執行

```bash
python3 scraper.py          # 抓取最新資料
python3 -m http.server 8000 # 啟動本地伺服器
# 打開 http://localhost:8000
```

## 部署

GitHub Pages 自動部署，每天自動檢查資料來源並更新資料。爬蟲會先讀 HackMD 索引頁，
再用搜尋結果作為備援來源尋找新一期 G3/B4 Google Sheets。
