#基于的基础镜像
FROM python:3.7

#代码添加到code文件夹
RUN mkdir -p /stock_code
COPY *.py /stock_code
COPY requirements.txt /stock_code
# stock_code
WORKDIR /stock_code
ENV PYTHONUNBUFFERED=0
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


# 安装支持
RUN pip3 install -r requirements.txt
CMD ["python3", "-u", "/stock_code/*.py"]
