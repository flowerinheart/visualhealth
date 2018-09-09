# -*- coding: utf-8 -*-
"""
主要功能：
从pm2.5in官网API接口获取实时气象数据，并写入pickle文件，同时记录日志文件。
1. 实时数据按月份并入历史数据的pickle文件（pandas.DataFrame格式）
2. 如需要，可用pickle_to_Excel.py脚本将pickle文件转成便于阅读的Excel文件

额外功能：
抓取失败的城市，稍后自动重新抓取

脚本执行方式：
1. 直接执行 - 抓取一次数据
2. 定时执行 - 将脚本部署在Linux云主机上，用crond设定定时执行该脚本（如每小时两次）


pm2.5in： 每小时刷新一次数据
API doc： http://www.pm25.in/api_doc


Created on Mon Jan 22 17:34:19 2018
@author: Raymond Zhang
"""

import requests
import json
from fake_useragent import UserAgent
import pandas as pd
import pickle
import datetime
import traceback
from sklearn.datasets import base
import time
import sys
import random
from bs4 import BeautifulSoup

ua = UserAgent()
def check_pre_update(CityList):
      """
      将上次update的内容载入以获取：
      1. 上次更新的时间点 （判断是否取消本次更新）
      2. 上次未更新的城市 （如有，以便这次重新抓取）
      """
      try:
#            _,_,time_point,preErrorCities = pd.read_pickle('AQIsData/update.pickle')
            pd.read_pickle('AQIsData/update.pickle')
            time_point = update.time
            preErrorCities = update.notUpdatedCity

      except:
            time_point = ''
            preErrorCities = []
      return time_point,preErrorCities



def get_proxy():
      r = requests.get('http://127.0.0.1:5000/get')
      proxy = BeautifulSoup(r.text, "lxml").get_text()
      return proxy


def get_province(data):
    input = "./province.json"
    json.load(open(input, "r"))

def download_data(CityList):
      print("begin")

      previous_time_point,preErrorCities = check_pre_update(CityList)

      # 先创建两个空的DataFrame格式的数据变量
      # Full_stations:该城市全站点数据      City_only：该城市概况数据
      Full_stations = City_only = pd.DataFrame()

      # API的访问站点
      url = 'http://www.pm25.in/api/querys/aqi_details.json'
      #url = 'http://www.pm25.in/api/querys/all_cities.json'
      token = '5j1znBVAsnSf5xQyNQyq'  # 公共token
      header = {'content-type': 'application/json; charset=utf-8',
                #'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0',
                'User-Agent': ua.random,
                'Connection': 'close'}

      NEW_TIME_POINT = True # 超参数：这次获取的是否为新数据
      ErrorCities = [] # 由于网络等原因未能下载到数据的城市，记录之并稍后再次更新
      update_cities_for_pre = [] # 此次更新的上次未更新之城市
      ErrorCities_for_pre = []   # # 此次未能更新的上次未更新之城市

      proxy_list = ['60.175.213.163:38513', '115.46.88.200:8123', '183.62.196.10:3128', '118.190.95.43:9001', '61.135.217.7:80', '118.190.95.35:9001', '106.75.9.39:8080', '114.113.126.86:80', '122.4.42.240:40587', '117.69.201.45:24331', '218.106.98.166:53281', '106.56.102.38:8070', '115.210.74.58:8010', '118.190.199.55:80', '106.56.102.113:8070', '121.31.194.250:8123', '115.217.252.117:25777', '115.215.50.71:43520']


      i = 0
      #for city in CityList:
      while i < len(CityList)   :
            city = CityList[i]
            i += 1
            # 【判断时间戳及未更新城市】 如果是上次的时间戳且该城市已更新过，则跳过
            print(str(i) + " " + city)
            #time.sleep(random.uniform(10, 19))
            if NEW_TIME_POINT == False and city not in preErrorCities:
                  continue
            else:
                  param  = {'city':city,'token':token}
                  try:
                        #ip = get_proxy()
                        #ip = proxy_list[i % len(proxy_list)]
                        #proxies = {"http" : ip}
                        #proxies = {"proxy" : ("https:\\" + ip)}
                        #print(proxies)
                        #r = requests.get(url, params = param,proxies=proxies, headers=header, timeout=10)
                        r = requests.get(url, params = param, headers=header, timeout=10)
                  except Exception as e:
                        if NEW_TIME_POINT == True:
                              ErrorCities.append(city)
                        else:
                              ErrorCities_for_pre.append(city)
                        log('[Request Error]  City: [{}] is unable to download.  --> Error: {}'.format(city,e))
                        continue
                  code = r.status_code
                  #判断是否通信成功，200代表成功
                  if code == 200:
      #                  log('GET request OK： {}'.format(city))
                        content = r.json() # request模块自带函数： 将json格式转成list格式
                        if isinstance(content, dict):
                              string = 'Sorry，您这个小时内的API请求次数用完了'
                              if list(content.keys())==['error'] and content['error'].startswith(string):
                                  log('[Failed]   token of API is out of use: {}'.format(city))
                              else:
                                  log('[Failed]   Unknown response for {}'.format(city))
#                              log(city+': '+str(content))
                              #return
                              #continue
                              #break
                              time.sleep(60 * 10)
                              i -= 1
                        elif isinstance(content, list):
                              # 获取此次更新的时间： 选择最后一条（for city），以免某些废站点的数据误导
                              time_point = content[-1]['time_point']
                              # 判断是否与上次获取数据的时间点,不同则执行此次更新
                              if time_point != previous_time_point:
                                    NEW_TIME_POINT = True
                                    city_data = pd.DataFrame(content).fillna('')
                                    Full_stations = Full_stations.append(city_data, ignore_index=True)
                                    # 从该城市全部站点中提取城市概况数据，记为City_only
                                    City_only = City_only.append(city_data.iloc[-1], ignore_index=True)
                              # 时间点相同
                              else:
                                    NEW_TIME_POINT = False
                                    # 如果没有上次未更新的城市，则取消本次更新，直接退出
                                    if len(preErrorCities) == 0:
                                          log('[Canceled] Same as the previous')
                                          return
                                    # 如有，则更新该城市的数据
                                    else:
                                          city_data = pd.DataFrame(content).fillna('')
                                          Full_stations = Full_stations.append(city_data, ignore_index=True)
                                          City_only = City_only.append(city_data.iloc[-1], ignore_index=True)
                                          if city in preErrorCities:
                                              update_cities_for_pre.append(city)

                  # 未访问成功
                  else:
                        if NEW_TIME_POINT == True:
                              ErrorCities.append(city)
                        else:
                              ErrorCities_for_pre.append(city)
                        log('[Request Error]    GET request error {}: {}'.format(code,city))

      # 更新新时间点的数据
      if NEW_TIME_POINT == True:
            if len(ErrorCities)==0:
                  infor = '[Success]  Updated all cities!       TimePoint: {}'.format(str(time_point))
            elif 0<len(ErrorCities)<len(CityList):
                  infor = '[Success]  Updated some of cities!   TimePoint: {}   --> Not updated: {}'.format(str(time_point),ErrorCities)
            elif len(ErrorCities)==len(CityList):
                  log('[Failed]   No cities are updated!')
                  return
            return [infor,[Full_stations,City_only,ErrorCities]]

      # 更新上次未更新的城市
      elif len(preErrorCities) != 0:
            if len(update_cities_for_pre) == 0:
                  log('[Failed]   No cities are updated for previous (TimePoint: {}) not-updated city(s)!'.format(previous_time_point))
                  return
            elif len(update_cities_for_pre) < len(preErrorCities):
                  infor = '[Success]  Updated: {}    Not updated: {}   TimePoint: {}'.format(update_cities_for_pre,ErrorCities_for_pre,str(time_point))
            elif len(update_cities_for_pre) == len(preErrorCities):
                  infor = '[Success]  Updated all previous not-updated cities!     TimePoint: {}   Updated: {}'.format(str(time_point),update_cities_for_pre)
            return [infor,[Full_stations,City_only,ErrorCities_for_pre]]


def add_province(cities):
      t = []
      with open("./city_province.json", "r") as input:
          province = json.load(input)
      for i in range(len(cities)):
            row = cities.iloc[i, :]
            city = row["area"]
            t.append(province.get(city))
      cities["province"] = t


def update_to_pickle(data):
      Full_stations,City_only,ErrorCities = data
      # 改变列顺序，使之更易读
      columns = ['time_point','area','position_name','station_code','aqi','quality',
                 'primary_pollutant','pm2_5', 'pm2_5_24h','pm10', 'pm10_24h',
                 'co', 'co_24h', 'no2', 'no2_24h', 'o3', 'o3_24h',
                 'o3_8h', 'o3_8h_24h','so2', 'so2_24h']
      Full_stations = Full_stations.reindex(columns=columns)
      City_only = City_only.reindex(columns=columns)
      City_only.pop('position_name')
      City_only.pop('station_code')
      time_point = City_only.iloc[0,0]
#      time_point = City_only.ix[0,'time_point']

      # 保存此次update的数据
      with open('AQIsData/update.pickle', 'wb') as file:
            data = base.Bunch(full = Full_stations,
                              city = City_only,
                              time = time_point,
                              notUpdatedCity = ErrorCities)
            pickle.dump(data, file)

      # 将更新并入历史数据
      month = time_point[:7]
      his_filename = '{}.pickle'.format(month) # 按月存放数据于一文件中
      filepath = 'AQIsData/'+his_filename
      import os
      if os.path.exists(filepath):
            try:
#                  Full_his, City_his, time_his = pd.read_pickle(filepath)
                  his = pd.read_pickle(filepath)
                  Full_his = his.full
                  City_his = his.city
                  time_his = his.time
            except Exception as e:
                  # 如无法获取该月份的历史数据，为了避免覆写历史数据的误操作，将本次更新的数据另建一pickle，以待后续手动合并
                  filename = 'not-merged-Data-{}.pickle'.format(time_point)
                  with open(r'AQIsData/'+filename, 'wb') as file:
#                        pickle.dump([Full_stations, City_only, time_point], file)
                        data = base.Bunch(full = Full_stations,
                                          city = City_only,
                                          time = time_point,
                                          notUpdatedCity = ErrorCities)
                        pickle.dump(data, file)
                  log('[Error]  Fail to load [{}] and unable to merge into his data. \
                                     Create an extra file:{}.  ({})'.format(his_filename,filename,e))
                  return
      else:
            #否则新建新月份的pickle文件
            Full_his = City_his = pd.DataFrame()
            time_his = pd.Series()
            log('=======================================================================================')
            log('[New his pickle] Create {}'.format(his_filename))
      # 合并之
      Full_his = pd.concat([Full_stations, Full_his], axis=0, join='outer', ignore_index=True)
      City_his = pd.concat([City_only, City_his], axis=0, join='outer', ignore_index=True)
      time_his = pd.Series(time_point).append(time_his,ignore_index=True)
      with open(filepath, 'wb') as file:
#            pickle.dump([Full_his, City_his, time_his], file)
            data = base.Bunch(full = Full_his,
                              city = City_his,
                              time = time_his)
            pickle.dump(data, file)

def log(infor):
      '''
      该函数记录日志文件
      infor: 本次记录之内容
      旧记录在后，新纪录在前（与传统方式略有不同，以便查看最新状况）
      '''

      filepath = r'AQIsData/{}.log'.format(time.strftime("%Y-%m"))

      # 先读入第二行后的内容
      try:
            with open(filepath, 'r') as f:
                  content = f.readlines()[2:]
      except:
            content = ''
      # 然后将本条记录插在旧记录前面，保持最新的永远在最前面
      with open(filepath, 'w') as f:
            head = '     Log Time       | Informaiton\n\n' #表头
            now = str(datetime.datetime.now())[:-7]
            update = '{} | {}\n'.format(now,infor)
            try:
                print(update)
            except Exception as e:
                print('print(update) occurs an error! --> {}'.format(e))
            f.write(head+update)   # 添加日志时间并写入
            f.writelines(content)  # 然后将之前的旧日志附在后面


def main():
      # 要抓取的城市，这里以广东九市为例
      #CityList = ['guangzhou','zhaoqing','foshan','huizhou','dongguan',
      #            'zhongshan','shenzhen','jiangmen','zhuhai']



      CityList = ['七台河', '三亚', '三明', '三沙', '三门峡', '上海', '上饶', '东莞', '东营', '中卫', '中山', '临夏州', '临安', '临汾','临沂', '临沧', '丹东', '丽水', '丽江', '义乌', '乌兰察布', '乌海', '乌鲁木齐', '乐山', '九江', '乳山', '云浮', '五家渠', '亳州', '伊春', '伊犁哈萨克', '伊犁哈萨克州', '佛山', '佳木斯', '保定', '保山', '信阳', '克州', '克拉玛依', '六安', '六盘水', '兰州', '兴安盟', '内江', '凉山州', '包头', '北京', '北海', '十堰', '南京', '南充', '南宁', '南平', '南昌', '南通', '南阳', '博州', '即墨', '厦门', '双鸭山', '句容', '台州', '合肥', '吉安', '吉林', '吐鲁番地区', '吕梁', '吴忠', '吴江', '周口', '呼伦贝尔', '呼和浩特', '和田地区', '咸宁', '咸阳', '哈密地区', '哈尔滨', '唐山', '商丘', '商洛', '喀什地区', '嘉兴', '嘉峪关', '四平', '固原', '塔城地区', '大兴安岭地', '大兴安岭地区', '大同', '大庆', '大理州', '大连', '天水', '天津', '太仓', '太原', '威海', '娄底', '孝感', '宁德', '宁波', '安庆', '安康', '安阳', '安顺', '定西', '宜兴', '宜宾','宜昌', '宜春', '宝鸡', '宣城', '宿州', '宿迁', '富阳', '寿光', '山南', '山南地区', '岳阳', '崇左', '巴中', '巴彦淖尔', '常州', '常德', '常熟', '平凉', '平度', '平顶山', '广元', '广安', '广州', '庆阳', '库尔勒', '廊坊', '延安', '延边州', '开封', '张家口', '张家港', '张家界', '张掖', '徐州', '德宏州', '德州', '德阳', '忻州', '怀化', '怒江州', '恩施州', '惠州', '成都', '扬州', '承德', '抚州', '抚顺', '拉萨', '招远', '揭阳', '攀枝花', '文山州', '文登', '新乡', '新余', '无锡', '日喀则','日喀则地区', '日照', '昆山', '昆明', '昌吉州', '昌都', '昌都地区', '昭通', '晋中', '晋城', '普洱', '景德镇', '曲靖', '朔州', '朝阳', '本溪', '来宾', '杭州', '松原', '林芝', '林芝地区', '果洛州', '枣庄', '柳州', '株洲', '桂林', '梅州', '梧州', '楚雄州', '榆林', '武威', '武汉', '毕节', '永州', '汉中', '汕头', '汕尾', '江门', '江阴', '池州', '沈阳', '沧州', '河池', '河源', '泉州', '泰安', '泰州', '泸州', '洛阳', '济南', '济宁', '海东地区', '海北州', '海南州', '海口', '海西州', '海门', '淄博', '淮北', '淮南', '淮安', '深圳', '清远', '温州', '渭南', '湖州', '湘潭', '湘西州', '湛江', '溧阳', '滁州', '滨州', '漯河', '漳州', '潍坊', '潮州', '濮阳', '烟台', '焦作', '牡丹江', '玉林', '玉树州', '玉溪', '珠海', '瓦房店', '甘南州', '甘孜州', '甘孜藏族自治州', '白城', '白山', '白银', '百色', '益阳', '盐城', '盘锦', '眉山', '石嘴山', '石家庄', '石河子', '福州', '秦皇岛', '章丘', '红河州', '绍兴', '绥化', '绵阳', '聊城', '肇庆', '胶南', '胶州', '自贡', '舟山', '芜湖', '苏州', '茂名', '荆州', '荆门', '荣成', '莆田', '莱州', '莱芜', '莱西', '菏泽', '萍乡', '营口', '葫芦岛', '蓬莱', '蚌埠', '衡水', '衡阳', '衢州', '襄阳', '西双版纳州', '西宁', '西安', '许昌', '诸暨', '贵港', '贵阳', '贺州', '资阳', '赣州', '赤峰', '辽源', '辽阳', '达州', '运城', '连云港', '迪庆州', '通化', '通辽', '遂宁', '遵义', '邢台', '那曲地区', '邯郸', '邵阳', '郑州', '郴州', '鄂尔多斯', '鄂州', '酒泉', '重庆', '金华', '金坛', '金昌', '钦州', '铁岭', '铜仁地区', '铜川', '铜陵', '银川', '锡林郭勒盟', '锦州', '镇江', '长春', '长沙', '长治', '阜新', '阜阳', '防城港', '阳江', '阳泉', '阿克苏地区', '阿勒泰地区', '阿坝州', '阿拉善盟', '阿里地区', '陇南', '随州', '雅安', '青岛', '鞍山', '韶关', '马鞍山', '驻马店', '鸡西', '鹤壁', '鹤岗', '鹰潭', '黄冈', '黄南州', '黄山', '黄石', '黑河', '黔东南州', '黔南州', '黔西南州', '齐齐哈尔', '龙岩']

      #CityList = ['平凉']
      # 伪装： 在Linux中采用crond方式定时抓数据时，将按整分钟执行，容易被服务器reject，故延迟一随机时间
      # 直接执行该脚本则无需time.sleep()
      #time.sleep(random.uniform(1, 19))

      data = download_data(CityList)

      # 如果data为空则无需update
      if data!=None:
            infor, updateData = data
            update_to_pickle(updateData)
            log(infor)


if __name__ == '__main__':
      try:
            main()
      except Exception:
            log('[Error] \n{}'.format(traceback.format_exc()))
