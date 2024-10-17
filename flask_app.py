from flask import request, render_template
from datetime import datetime
from os.path import join
import io
from logic import csv, pd, DataseriesDB, DataentryDB, readfromfolderpandas, savetofilespandas, savetofiles, savetofilessql, Dataentry, Dataseries, app, db, datascrepancy, readfromfolder, DateTime, readfromfolderog, serieslist,URLlist
import logic
import time

@app.route("/log")
def log():
    f = open("dataseries/log.txt", "r")
    page= ""
    myline = f.readline()
    while(myline):
        page +=myline+"\n<br>"
        myline = f.readline()
    return page

@app.route("/initialize")
def initialize():
    db.create_all()
    DataentryDB.query.delete()
    DataseriesDB.query.delete()
    db.session.commit()
    page="WYKONANE"
    mypath = "dataseries"
    readfromfolder(mypath)
    return page

@app.route("/data/<id>")
def data(id):
    global h
    page= ""
    zh = DataseriesDB.query.filter_by(series=id).first()
    if zh is not None:
        for i in zh.legions:
            page+=id+' '+i.print()+' '+str(i.value)+"<br>\n"
    else: page += "No such series"
    return page

@app.route("/serwer", methods=['GET','POST'])
def main():
    args = request.form
    if args.get("type"):
        logic.separator = args.get("type")
    else:
        logic.separator = ','
    appending = bool(0)
    updating = bool(0)
    if "dateformat" in args:
        logic.dateformat = args.get("dateformat")
    if args.get("appending"):
        appending = bool(1)
    if args.get("updating"):
        updating = bool(1)
    mypath = "dataseries"
    checked=logic.dateformat
    checked1 = ['','']
    if appending: checked1[0]='checked'
    if updating: checked1[1]='checked'
    checktypes=""
    if "types" in args:
        checktypes = args.get("types")
        assert checktypes=='sql' or checktypes=='man' or checktypes=='pan'
    checkedtypes = ['','checked','']
    if(checktypes=="sql"): checkedtypes = ['checked','','']
    elif(checktypes=="man"): checkedtypes = ['','checked','']
    elif(checktypes=="pan"): checkedtypes = ['','','checked']
    global pandaseries
    global pandaentries
    if(checktypes=='man'):
        serieslist.clear()
        readfromfolderog(mypath)
    elif(checktypes=='pan'):
        URLlist.clear()
        xj = readfromfolderpandas(mypath)
        pandaentries = xj

    files = []
    if(args.get("default")):
        files.append(open("insert.csv"))
    elif(len(request.files)>0):
        files = request.files['myfile']
    hjk = dict()
    g = open("dataseries/log.txt", "a")
    g.write('\n'+str(datetime.now()))
    global start
    start = time.time()
    for f in files:
        if(args.get("default")):
            csv_reader = csv.reader(f,delimiter=logic.separator)
        else:
            csv_reader = csv.reader(io.StringIO(f.decode('utf-8')), delimiter=logic.separator)
        for x in csv_reader:
            if(len(x)!=3):
                g.write('\n'+'Data mismatch. Wrong format of data series in line: "'+x+'"');
            else:
                x[1] = x[1].strip()
                x[0] = x[0].strip()
                x[2] = x[2].strip()
                if(checktypes=='sql'):
                    try:
                    	new_vc = Dataentry(date=DateTime(x[1]),value=x[2])
                    except ValueError:
                    	g.write('\n'+'Format error:'+x[1])
                    	break;
                    hg = None			
                    if x[0] in hjk.keys():
                        hg = hjk[x[0]]
                    elif DataseriesDB.query.filter_by(series=x[0]).count() > 0:
                        hg = DataseriesDB.query.filter_by(series=x[0]).first()
                        hjk[x[0]] = hg
                    if hg is not None:
                        if x[1] in hg.legions.keys():
                            if updating:
                                xx = hg.legions[x[1]]
                                if(datascrepancy(xx.value,new_vc.value)):
                                    g.write('\n'+'Data mismatch in: '+x[0]+' '+xx.print()+'. New value ('+str(new_vc.value)+') too different from '+str(xx.value));
                                else:
                                    v = xx.value
                                    xx.value = new_vc.value
                                    g.write('\n'+'In '+x[0]+' '+xx.print()+' replaced '+str(v)+' with new value: '+ str(new_vc.value));
                        elif appending:
                            hg.legions[x[1]] = new_vc
                            g.write('\n'+'Added new line: '+x[0]+', '+x[1]+', '+x[2])
                    elif appending:
                        new_h = DataseriesDB(series=x[0],URLpath=join(mypath,x[0]+'.csv'))
                        new_h.legions = dict()
                        new_h.legions[x[1]]=new_vc
                        db.session.add(new_h)
                        g.write('\n'+'Added new line: '+x[0]+', '+x[1]+', '+x[2])

                if(checktypes=='man'):
                    try:
                    	new_jj = Dataentry(date=DateTime(x[1]),value=x[2])
                    except ValueError:
                    	g.write('\n'+'Format error:'+x[1])
                    	break;
                    if x[0] in serieslist.keys():
                        if x[1] in serieslist[x[0]].legions.keys():
                            if updating:
                                xx = serieslist[x[0]].legions[x[1]]
                                if(datascrepancy(xx.value,new_jj.value)):
                                    g.write('\n'+'Data mismatch in: '+x[0]+' '+xx.print()+'. New value ('+str(new_jj.value)+') too different from '+str(xx.value));
                                else:
                                    v = xx.value
                                    xx.value = new_jj.value
                                    g.write('\n'+'In '+x[0]+' '+xx.print()+' replaced '+str(v)+' with new value: '+ str(new_jj.value));
                        elif appending:
                            serieslist[x[0]].legions[x[1]] = new_jj
                            g.write('\n'+'Added new line: '+x[0]+', '+x[1]+', '+x[2])
                    elif appending:
                        new_h = Dataseries(id=x[0],legions=dict(),URLpath=join(mypath,x[0]+'.csv'))
                        new_h.legions[x[1]] = new_jj
                        serieslist[x[0]]=new_h
                        g.write('\n'+'Added new line: '+x[0]+', '+x[1]+', '+x[2])

                if(checktypes=='pan'):
                    try:
                    	new_jj = Dataentry(date=DateTime(x[1]),value=x[2])
                    except ValueError:
                    	g.write('\n'+'Format error:'+x[1])
                    	break;
                    if x[0] in URLlist.keys():
                        temp = pandaentries.loc[pandaentries['Series']==x[0]]
                        if x[1] in temp['Date'].values:
                            if updating:
                                old_value = str(pandaentries.loc[(pandaentries['Date']==x[1]) & (pandaentries['Series']==x[0]),'Value'].iloc[0])
                                if(datascrepancy(old_value,x[2])):
                                    g.write('\n'+'Data mismatch in: '+x[0]+' '+x[1]+'. New value ('+str(x[2])+') too different from '+str(old_value));
                                else:
                                    pandaentries.loc[(pandaentries['Date']==x[1]) & (pandaentries['Series']==x[0]),'Value'] = x[2]
                                    g.write('\n'+'In '+x[0]+' '+x[1]+' replaced '+old_value+' with new value: '+ x[2]);
                        elif appending:
                            df = pd.DataFrame(data=[x])
                            df.columns = ['Series', 'Date', 'Value']
                            pandaentries = pd.concat([pandaentries,df])
                            g.write('\n'+'Added new line: '+x[0]+', '+x[1]+', '+x[2])
                    elif appending:
                        df = pd.DataFrame(data=[x])
                        df.columns = ['Series', 'Date', 'Value']
                        pandaentries = pd.concat([pandaentries,df])
                        URLlist[x[0]]=join(mypath,x[0]+'.csv')
                        g.write('\n'+'Added new line: '+x[0]+', '+x[1]+', '+x[2])
                
    if(checktypes=='sql'):
        db.session.commit()
        savetofilessql(DataseriesDB.query.filter(DataseriesDB.series.in_(hjk.keys())))
    elif(checktypes=='man'):
        savetofiles(serieslist.values())
    elif(checktypes=='pan'):
        savetofilespandas(pandaentries)
    passed = -1
    if(len(checktypes)>0):
        g.close()
        end = time.time()
        passed = float(int(100*(end - start))/100)

    return render_template("index.html", checked=checked, checkedtypes=checkedtypes,passed = passed,  checked1=checked1)
	#return page;
