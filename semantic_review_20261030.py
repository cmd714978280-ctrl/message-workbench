"""Persist this session's independent content review, not classifier self-scoring."""
import json
from pathlib import Path
from ingestion_store import IngestionStore

EXPECTED = {
 'warning': [431,432,483,603,792,793],
 'mini': [669,673],
 'service': [794,824,801,802,803,804,805,806,807,808,809,810,1098],
 'requirement': [421,422,425,426,433,436,441,444,482,496,502,635,648,649,652,653,664,665,695,696,700,776,777,782,790,791,811,883,918,932,996,997],
 'product_link': [529,538,650,717,769,780,781],
 'thanks': [508,509,565,835],
 'feedback': [427,428,486,487,542,543,551,552,557,558,563,562,566,567,568,569,601,645,646,647,654,655,662,663,832,833,834],
 'order_image': [423,438,484,511,532,540,651,745,783,881,903,992],
 'unknown': [598,599,600],
 'ordinary': [424,429,442,481,506,510,514,541,581,582,597,632,633,634,636,667,668,670,671,672,674,675,701,721,723,991],
 'closing': [430,434,435,437,485,497,507,512,513,528,530,531,533,561,574,575,576,579,580,596,602,604,618,637,666,676,697,715,716,720,784,800,879,995],
 'qa': [478,501,525,539,550,577,578,583,614,694,698,699,712,737,738,774,785,812,813,814,815,912,993],
}
NOTES = {
 'warning': '结合原文，存在群外服务招揽、推广卡片、现金提现活动或银行营销；不是本群商品答疑，应使用撤回规则。',
 'mini': '来源正文明确说明是小程序分享，按已确认群规处理。',
 'service': '结合客户的退货、退款寄件、商家拦截退回或食品异物投诉及图片，属于售后客服处理的客户问题；实拍和订单页面在这里是投诉证据，不能入晒单/订单宣传图库。',
 'requirement': '客户在寻找、回购或等待某类商品，需要核对排品/历史活动并交一对一客服或需求登记；不是通用FAQ答疑，也不能把商品参考图当成晒单。',
 'product_link': '客户直接寻找群内活动或商品的购买入口；应查对应日期商品文案和购买链接，链接未确认时保留待补资料。',
 'thanks': '客户明确@豆妈并表达感谢，没有新增问题，适当回应即可。',
 'feedback': '同一客户的商品实拍与收货/使用配文构成反馈材料，图片和文案应在同一会话保留。这里只认定收录候选，不执行上传。',
 'order_image': '直接查看图片后，属于正常购买成功/待发货订单记录，附近没有退款、质量或物流求助；可作为订单图候选。',
 'unknown': '图片无法取回，或仅有生活照片而缺少商品关联，现有证据不足以认定订单/晒单/广告；保留明确的不确定性。',
 'ordinary': '结合完整会话，属于使用感受、闲聊、补充陈述或非问题信息；没有新增客服任务。关联配文仍应保留在所属会话。',
 'closing': '当前是已拍/已下单/已找到、简短感谢确认或误触发说明，没有新问题；不应因旧问题或单独@豆妈再次转答疑。',
 'qa': '属于当前商品使用、成分/适用人群、款式拍单操作或连续追问，需要依据有效商品资料或人工处理；不能自行补造产品事实。',
}
SPECIAL = {
 431:'文字是在推荐追债顾问并邀请联系，且同秒发推广卡片；即使句首“有没有”也不能当答疑。',
 793:'与同一客户同秒律所卡片组成一段群外引流广告，“免费咨询、加他微信、帮忙宣传”是明确招揽。',
 603:'明确推广快手极速版现金提现、边玩边赚，与本群业务无关，应要求撤回。',
 794:'图片实际为退货寄件/运费页面，同一客户随后投诉产品有异物；是售后证据，不是正常下单晒单。',
 803:'“我的货怎么都被商家召回了”明确是已购商品物流异常，截图也显示商家拦截、包裹退回。',
 807:'“这样的产品这样的资量能让人放心吃吗”结合多张圈出毛发/异物的包装图，是质量投诉；不作商品质量真伪鉴定。',
 810:'“头发”是前述产品异物投诉的补充说明，不是孤立闲聊。',
 653:'牙膏实物图是紧邻“这个牙膏什么时候可以回购”的识别材料，不能作为独立正面晒单进图库。',
 650:'“卫衣在哪里拍，我也想要”主要在找当天购买入口，不能被“想要”单词抢成需求登记。',
 715:'“找到了，已拍”明确解决前面的款式查找问题，@豆妈也不构成新的问题。',
 531:'“已拍早餐，太划算了”是购买确认，单独@豆妈不应改成答疑。',
 539:'“来迟了，喜欢的款式没有了”是在描述拍单/款式缺货困扰，需要人工核对，不是纯闲聊。',
 648:'图片是面条/餐食，紧接“什么时候还有”，业务意图是询问对应商品供给，不能仅因实拍就入反馈图库。',
 664:'商品包装宣传图与“还有链接吗”是一组历史商品复购咨询，不能仅凭图中的商品名套当天排品。',
 598:'儿童生活照未给出商品名、已购或使用评价，不能自行认定是童装反馈；相邻同组照片同理。',
 782:'虽然前半句是已买次抛并感谢，末尾新增“防晒霜有吗”的商品需求，不能直接无需回复。',
 812:'“涂不开”结合后续“化不开、你有这个感觉吗”是产品使用困惑，需核实用法或交客服；未证明是质量故障。',
 813:'结合前后同一客户的涂不开问题，是连续追问，不宜当作独立普通闲聊。',
 814:'“腻住、化不开”是同一段产品使用问题的补充，保留人工答疑上下文。',
 815:'明确向他人询问是否也有同样使用感受，保留产品使用答疑语境。',
 442:'“回购MY”在这里没有问句或索要入口，暂按购买意向陈述保留；不强行认定为已下单或要客服处理。',
 701:'“我们又搞活动了”信息太少，未见外部链接或招揽对象；不能仅凭“活动”判广告。',
 670:'附件实际返回audio/mpeg，是语音而非图片；同一客户随后明确说明“宝宝玩手机发的”，按误触发普通消息保留，不猜测语音内容。',
 671:'同一段宝宝误触发的语音消息；实际资源为audio/mpeg，不能统计成图片识别失败。',
 672:'同一段宝宝误触发的语音消息；实际资源为audio/mpeg，不能统计成图片识别失败。',
 674:'同一段宝宝误触发的语音消息；实际资源为audio/mpeg，不能统计成图片识别失败。',
 675:'同一段宝宝误触发的语音消息；实际资源为audio/mpeg，不能统计成图片识别失败。',
 932:'客户询问昨天纸巾活动是否仍能拍，是历史活动需求；此次已找回明确关联的人工回复及现有入口，保留该事实，不凭分类新增客服转交。',
 1098:'客户在退货寄件截图后询问为什么退货还收运费，属于售后运费处理；真实回复也指向小助理报销运费。',
}


def main():
    root=Path(Path('data/reports/latest-semantic-audit.txt').read_text(encoding='utf-8'))
    source=json.loads((root/'messages.json').read_text(encoding='utf-8'))
    expected={seq:label for label,seqs in EXPECTED.items() for seq in seqs}
    inbound={x['sequence']:x for x in source['messages'] if x['direction']=='inbound'}
    if set(inbound)!=set(expected):
        raise ValueError(f'Missing review: {set(inbound)-set(expected)}; unknown sequences: {set(expected)-set(inbound)}')
    store=IngestionStore(Path('data/messages.sqlite3'))
    decisions=[]
    for seq,item in inbound.items():
        label=expected[seq];reason=SPECIAL.get(seq,NOTES[label]);missing=any(m['state'] in ('failed','pending','retry') for m in item['media'])
        if missing:
            reason+=' 原图不可用：本条只按明确的客户文字意图判断，禁止当作已检查图片或直接上传。'
        old=item['analysis'].get('primary');changed=old!=label
        review_applied=False
        current_review=store.analysis_map([seq],source['rule_version'])[1].get(seq) or {}
        if changed and (not current_review or current_review.get('reviewer')=='Codex 上下文语义复核'):
            store.save_review(seq,'approved',label,[label],reason,reviewer='Codex 上下文语义复核',rule_version=source['rule_version'])
            review_applied=True
        decisions.append({'sequence':seq,'timestamp':item['source_timestamp'],'group':item['group'],'sender':item['sender'],
            'content':item['content'],'predicted_primary':old,'expected_primary':label,'changed':changed,
            'reason':reason,'media_unavailable':missing,'applied_classification_only':review_applied,
            'confidence':'unresolved' if label=='unknown' else 'high','reviewer':'Codex 上下文语义复核'})
    counts={label:sum(x['expected_primary']==label for x in decisions) for label in EXPECTED}
    report={'scope_start':source['start'],'scope_end':source['started_at'],'review_basis':'本轮助手逐条阅读原文、同客户会话、真实回复和可用原图，独立判断业务含义；不是规则重跑自评。',
        'reviewed':len(decisions),'changed':sum(x['changed'] for x in decisions),'unresolved':sum(x['expected_primary']=='unknown' for x in decisions),
        'counts':counts,'decisions':decisions}
    (root/'semantic-review.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k!='decisions'},ensure_ascii=False))


if __name__=='__main__':main()
