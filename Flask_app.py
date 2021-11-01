from flask import Flask,request,render_template
from Utils import Model_prediction

app = Flask(__name__)

@app.route('/')
def news_page1():
    return render_template("News_Predictor.html")


@app.route('/news_predictor',methods=['POST','GET'])
def prediction_funct():
    param_txt=request.form["summary_text"]
    #print(param_txt)
    if request.method == "GET" :
        return "Try POSTing instead"
    result=Model_prediction(param_txt)
    return render_template("output_display_page.html",output=result,Text_recieved=param_txt)

if __name__=='__main__':
    app.run()


