# -*- coding: utf-8 -*-
from tgrocery import Grocery

grocery = Grocery('sample')
grocery.train('motion')
grocery.save()

new_grocery = Grocery('sample')
new_grocery.load()

#print new_grocery.predict('安排 发货 为您服务 请稍等 客服 返钱 购物 访客 小样 回复 订单 专线 提示 网络 会查')

test_src = [
    ('美式乡村','客服 停运 发货 恭喜发财 为您服务 请稍等 春节 这边 海滨 售后 物流 提示 专员 飞吻 鸡年 链接 五包 帮到 送装 拜个'),
    ('简约现代','发货 客服 叶果 那款 专属 货期 悲泣 休息 可以 没来 玫瑰 联系 下班 了解 回复 订单 来货 尽快 敬请原谅'),
    ('美式古典','安装 电梯 访客 收货 小样 送货 自助 回复 细节 请稍等 勾选 及时 抱歉 微笑 务必 购物 问题 订单 师傅'),
]

print new_grocery.predict('试用了几天，总体来说感觉不错，做工也可以，售后也好，客服也态度很好，安装师傅安装的也快，态度也可以，值这个价')

print new_grocery.predict('不好意思，打错字了，原谅我的粗心。不是沙幕，是客服沙慕的服务不错，非常满意就可以。')
print new_grocery.predict('床很大方很显小也很实用，价格适中，虽然等了一个月 还是值得，感谢客服飞舞，还会购买的')
print new_grocery.predict('客服说给好评就返20元，结果没有返，骗人的，骗好评')
