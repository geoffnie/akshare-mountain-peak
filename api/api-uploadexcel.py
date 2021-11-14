from flask import Flask, request
import xlrd

app = Flask(__name__)

# 处理中文编码
app.config['JSON_AS_ASCII'] = False


# 跨域支持
def after_request(resp):
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


app.after_request(after_request)


# 上传表格
@app.route("/excel_info", methods=["GET", "POST"])
def excel_info_():
    if request.method == "POST":
        #  获取参数用request.form，获取文件用request.files
        file = request.files.get('file')
        if not file:
            return {"code": '401', "message": "缺少参数"}
        # 读取表格内容
        workbook = xlrd.open_workbook(file_contents=file.read())
        # 取第一个sheet
        sheet = workbook.sheet_by_index(0)
        # 获取总行数
        row = sheet.nrows
        # 从表格中选取字段
        titles = ['name', 'age', 'address']
        json_list = []
        # 遍历每一行的内容
        for i in range(row):
            if i == 0:
                continue
            row_value = sheet.row_values(i)
            # 构造字典
            obj = dict()
            json_list.append(obj)
            for title, col_val in zip(titles, row_value):
                obj.setdefault(title, col_val)
        # 将读取的内容作为结果返回
        return {"code": '200', "message": json_list}
    else:
        return {"code": '403', "message": "仅支持post方法"}


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)