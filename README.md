# angelhunt
# VisualHealth
An android app use to urge the government to reduce pollution.I aim to achieve goal 3 "GOOD HEALTH AND WELL-BEING" and goal 6 "clean water and sanitation".

Here is my plan.
1. Build a scrawler to get air quality data from "https://www.aqistudy.cn/". [Finished]
2. Build a scrawler to get water quality data from "http://www.cnemc.cn/sssj2092874.jhtml". [Unfinished]
3. Use django as web framework to provide rest api for android app. [In progress]
4. Build a android app to display air quality of each city and province. [In progress]
5. Design a machine learning modle to predict future air quality. [Unfinshed]

Now you just can run android app which load demo data, backend isn't finished.
## Techonology
* Convert svg to canvas path structure
* Canvas draw map
* RxJava
* NavigationTabBar
* Python crawler
* Django
* Tensorflow, LSTM

## Screenshot

<table align="center">
    <tr>
    <td>
      <img src="./app/screenshot/a.jpg" height="400" width="200">
    </td>
    <td>
      <img src="./app/screenshot/b.jpg" height="400" width="200">
    </td>
    </tr>
</table>


* First page display air/water quality of each province.[Finished]
* Second page display air/water quality of user's city in detail.[Unfinished]
* Third page display news about air and water pollution.[Unfinished]
* Fourth page display predicted air/waiter quality of next dtay and give some remind.[Unfinished]
* Fifth page show settings.[Unfinished]
