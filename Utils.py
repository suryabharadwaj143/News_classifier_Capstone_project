import pickle
import pandas as pd


def Model_prediction(docs_new):
    f = pd.read_csv("Topics_pred.csv",header=None)
    dict_encoded_pred=dict(zip(f[0].to_list(), f[1].to_list()))
    
    model=pickle.load(open('News_Clf_Model.pkl','rb'))
    
    # Prediction

    predicted=model.predict([docs_new])
    #print(dict_encoded_pred[predicted[0]])
    return dict_encoded_pred[predicted[0]]

#print(Model_prediction("The World Health Organization (WHO) announced 26 proposed members to an advisory committee aimed to steer studies into the origin of the COVID-19 pandemic and other pathogens of epidemic potential"))

    