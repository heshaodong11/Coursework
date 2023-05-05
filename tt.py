from elasticsearch import Elasticsearch
import numpy as np
from pyecharts import options as opts
from pyecharts.charts import Bar, Grid, Line, Liquid, Page, Pie,Funnel,WordCloud,Map
from langdetect import detect, LangDetectException


# 定义查询函数
def esQuery(indexName):
    body = {
        "query": {
            "match_all": {}
        }
    }
    # 设置scroll参数
    scroll = '5m'  # 滚动查询超时时间
    size = 1000  # 每页返回记录数

    # 执行第一次查询
    result = es.search(index=indexName, body=body, scroll=scroll, size=size)

    # 取得总记录数
    total = result['hits']['total']['value']

    # 取得第一页结果
    hits = result['hits']['hits']

    # 循环滚动游标，取得所有结果
    while len(hits) < total:
        # 获取scroll_id
        scroll_id = result['_scroll_id']

        # 使用scroll API获取下一页结果
        result = es.scroll(scroll_id=scroll_id, scroll=scroll)

        # 取得下一页结果
        hits += result['hits']['hits']

    rdrUserName = []
    rdrFeelValue = []
    rdrAssetCount = []
    rdrLikeType = []
    rdrTotalTime = []
    rdrComment = []
    rdrPoint = []

    # 输出所有结果
    for hit in hits:
        rdrUserName.append(hit['_source']['userName'])
        rdrFeelValue.append(hit['_source']['feelValue'])
        rdrAssetCount.append(hit['_source']['assetCount'])
        rdrLikeType.append(hit['_source']['likeType'])
        rdrTotalTime.append(hit['_source']['totalTime'])
        rdrComment.append(hit['_source']['comment'])
        rdrPoint.append(hit['_source']['point'])
    return rdrUserName,rdrFeelValue,rdrAssetCount,rdrLikeType,rdrTotalTime,rdrComment,rdrPoint

# 按照point排序
def queryByPoint(indexName):
    body = {
        "query": {
            "match_all": {}
        },
        "size": 1000,
        "sort": [
            {
                "point": {
                    "order": "desc"
                }
            }
        ]
    }

    result = es.search(index=indexName, body=body)

    # 取得总记录数
    total = result['hits']['total']['value']

    # 取得第一页结果
    hits = result['hits']['hits']
    rdrComment = []
    rdrPoint = []
    for hit in hits:
        # if(len(rdrComment)>20):
        #     break
        try:
            if len(hit['_source']['comment'])<40 and detect(hit['_source']['comment']) != 'zh-cn' and detect(hit['_source']['comment']) != 'zh-tw' and hit['_source']['comment']!="给女朋友口了三小时才给我买的":
                rdrComment.append(hit['_source']['comment'])
                rdrPoint.append(hit['_source']['point'])
        except LangDetectException as e:
            continue
            # print("识别到图片品论或者表情评论，无法判断语种！！！")
        finally:
            continue

    return rdrComment,rdrPoint

# 评论长度范围查询
def commentLength(commentRange,indexName):
    commentRangeCount = []
    # 按照评论长度进行查询
    for i in range(len(commentRange) - 1):
        query = {
            "query": {
                "script": {
                    "script": {
                        "source": "doc['comment'].size() > params.min_length && doc['comment'].size() <= params.max_length",
                        "lang": "painless",
                        "params": {
                            "min_length": commentRange[i],
                            "max_length": commentRange[i + 1]
                        }
                    }
                }
            }
        }
        result = es.count(index=indexName, body=query)
        commentRangeCount.append(result['count'])
    return commentRangeCount

# 按照玩家游玩时间进行查询
def playTimeRange(timeRange,indexName):
    timeRangeCount = []
    for i in range(len(timeRange) - 1):
        # 按照玩家游玩时间范围分类
        body = {
            "query": {
                "range": {
                    "totalTime": {
                        "gte": timeRange[i],
                        "lte": timeRange[i + 1]
                    }
                }
            }
        }
        result = es.count(index=indexName, body=body)
        timeRangeCount.append(result['count'])
    return timeRangeCount

def findLikeType(likeTypeTitle,indexName):
    likeTypeCount = []
    for likeType in likeTypeTitle:
        body = {
            "query": {
                "match": {
                    "likeType.keyword": likeType
                }
            }
        }
        result = es.count(index=indexName, body=body)
        likeTypeCount.append(result['count'])
    return likeTypeCount

def drawComment(commentRangeTitle,game1title,game1CommentRangeCount,game2title,game2CommentRangeCount,description)->Bar:
    c = (
        Bar()
        .add_xaxis(commentRangeTitle)
        .add_yaxis(game1title,game1CommentRangeCount)
        .add_yaxis(game2title, game2CommentRangeCount)
        .set_global_opts(
            title_opts=opts.TitleOpts(title=description)
        )
    )
    return c

def drawPie(game1Title,game1Count,game2Count,)->Pie:
    c=(
        Pie()
        .add(
            "",
            [list(z) for z in zip(game1Title, game1Count)],
            radius=["30%", "75%"],
            center=["25%", "50%"],
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add(
            "",
            [list(z) for z in zip(game1Title, game2Count)],
            radius=["30%", "75%"],
            center=["75%", "50%"],
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="荒野大镖客2与战神4推荐与不推荐对比")
        )
    )
    return c

def drawBar(gameTitle,gameXValue,gameYValue,description)->Bar:
    c = (
        Bar()
        .add_xaxis(gameXValue)
        .add_yaxis(gameTitle, gameYValue)
        .set_global_opts(
            title_opts=opts.TitleOpts(title=description),datazoom_opts=opts.DataZoomOpts(),
        )
    )
    return c

def drawWorldCloud(data,description)->WordCloud:
    c=(
        WordCloud()
        .add(series_name=description, data_pair=data, word_size_range=[6, 66])
        .set_global_opts(
            title_opts=opts.TitleOpts(
                title=description, title_textstyle_opts=opts.TextStyleOpts(font_size=23)
            ),
            tooltip_opts=opts.TooltipOpts(is_show=True),
        )
    )
    return c

def drwaMap(country,value,description)->Map:
    c = (
        Map()
        .add(description, [list(z) for z in zip(country, value)], "world")
        .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
        .set_global_opts(
            title_opts=opts.TitleOpts(title=description),
            visualmap_opts=opts.VisualMapOpts(max_=200),
        )
    )
    return c
if __name__ == '__main__':
    godofwarlanguageCount = {'zh-cn':0,'zh-tw':0,'ja':0,'ko':0,'th':0,'bg':0,'cs':0,'da':0,'de':0,'en':0,'es':0,'el':0,'fr':0,'it':0,'hu':0,'nl':0,'no':0,'pl':0,
                     'pt':0,'ro':0,'ru':0,'fi':0,'sv':0,'tr':0,'vi':0,'uk':0,'et':0,'sw':0,'ca':0,'so':0,'cy':0};
    rdrlanguageCount = {'zh-cn': 0, 'zh-tw': 0, 'ja': 0, 'ko': 0, 'th': 0, 'bg': 0, 'cs': 0, 'da': 0, 'de': 0,
                     'en': 0, 'es': 0, 'el': 0, 'fr': 0, 'it': 0, 'hu': 0, 'nl': 0, 'no': 0, 'pl': 0,
                     'pt': 0, 'ro': 0, 'ru': 0, 'fi': 0, 'sv': 0, 'tr': 0, 'vi': 0, 'uk': 0, 'et': 0, 'sw': 0, 'ca': 0,
                     'so': 0, 'cy': 0};
    countryName = ['China','Taiwan','Japan','Korea','Thailand','Bulgaria','Czech Republic','Denmark','Germany','United Kingdom','Spain','Greece','France','Italy','Hungary',
                   'Netherlands','Norway','Poland','Portugal','Romania','Russia','Finland','Sweden','Turkey','Vietnam','Ukraine','Estonia','Swahili','Catalonia','Somalia','Wales'];


    # 创建 Elasticsearch 实例
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}], timeout=60, max_retries=3, retry_on_timeout=True)

    # 评论长度对比
    commentRange = [0, 10, 20, 50, 100, 1000]
    commentRangeTitle = ['0-10', '10-20', '20-50', '50-100', '100-1000']
    rdrCommentRangeCount = commentLength(commentRange, 'rdr')
    godOfWarCommentRangeCount = commentLength(commentRange, 'godofwar')

    godofwarUserName,godofwarFeelValue,godofwarAssetCount,godofwarLikeType,godofwarTotalTime,godofwarComment,godofwarPoint = esQuery('godofwar')

    for comment in godofwarComment:
        try:
            godofwarlanguageCount[detect(comment)] +=1
        except LangDetectException as e:
            continue
            # print("识别到图片品论或者表情评论，无法判断语种！！！")
        finally:
            continue

    rdrUserName, rdrFeelValue, rdrAssetCount, rdrLikeType, rdrTotalTime, rdrComment, rdrPoint = esQuery(
        'rdr')

    for comment in rdrComment:
        try:
            rdrlanguageCount[detect(comment)] += 1
        except LangDetectException as e:
            continue
            # print("识别到图片品论或者表情评论，无法判断语种！！！")
        finally:
            continue


    # 计算平均时间
    avgTimes = np.array(rdrTotalTime)
    avgTime = np.mean(avgTimes)
    avgTotalTime = []
    for totalTime in rdrTotalTime:
        avgTotalTime.append(int(totalTime - avgTime))

    godofwaravgTimes = np.array(godofwarTotalTime)
    godofwaravgTime = np.mean(godofwaravgTimes)
    godofwaravgTotalTime = []
    for totalTime in godofwarTotalTime:
        godofwaravgTotalTime.append(int(totalTime - godofwaravgTime))

    # 按照游玩时长进行分类
    timeRange = [0, 10, 20, 30, 40, 50, 100, 200,500]
    timeRangeTitle = ['0-10', '10-20', '20-30', '30-40', '40-50', '50-100', '100-200','200-500']
    rdrTimeRangeCount = playTimeRange(timeRange, 'rdr')
    godOfWarTimeRangeCount = playTimeRange(timeRange, 'godofwar')

    # 按照是否推荐进行分类统计
    likeTypeTitle = ['推荐', '不推荐']
    rdrLikeTypeCount = findLikeType(likeTypeTitle, 'rdr')
    
    godOfWarLikeTypeCount = findLikeType(likeTypeTitle, 'godofwar')

    rdrComment,rdrPoint = queryByPoint('rdr')
    rdrz = zip(rdrComment, rdrPoint)

    godofwarComment, godofwarPoint = queryByPoint('godofwar')
    godofwarz = zip(godofwarComment, godofwarPoint)



    page = Page(layout=Page.DraggablePageLayout)
    page.add(drawComment(commentRangeTitle,"荒野大镖客2", rdrCommentRangeCount,"战神4", godOfWarCommentRangeCount,"荒野大镖客2与战神4评论长度对比"),
             drawComment(timeRangeTitle,"荒野大镖客2", rdrTimeRangeCount,"战神4", godOfWarTimeRangeCount,"荒野大镖客2与战神4游玩时长对比"),
             drawBar("荒野大镖客2",rdrUserName,avgTotalTime,"荒野大镖客2玩家游戏时长对比"),
             drawBar("战神4", godofwarUserName, godofwaravgTotalTime, "战神4玩家游戏时长对比"),
             drawPie(likeTypeTitle,rdrLikeTypeCount,godOfWarLikeTypeCount),
             drwaMap(countryName,godofwarlanguageCount.values(),"战神4玩家分布情况"),
             drwaMap(countryName, rdrlanguageCount.values(), "荒野大镖客2玩家分布情况"),
             drawWorldCloud(rdrz,"荒野大镖客2词云"),
             drawWorldCloud(godofwarz,"战神4词云"),)
    page.render("荒野大镖客2vs战神4.html")

