# 5300-Fossa

## Sprint Invierno Shrividya & Adama
Start by cloning the repo to CS1 from 
```
git clone https://github.com/klundeen/5300-Fossa.git
```
### To run Milestone 5:
On CS1:
```
cd cpsc5300
rm -f data/*
cd 5300-Fossa
git checkout tags/Milestone5 
make
```
To run the program:
```
./sql5300 ~/cpsc5300/data
```

### To run Milestone 6:
On CS1:
```
cd cpsc5300
rm -f data/*
cd 5300-Fossa
git checkout tags/Milestone6 
make
```
To run the program:
```
./sql5300 ~/cpsc5300/data
```


Test runs for lookup:

Note: We need to delete the content 'foo-fxx.db' in 'data' directory before running the second 'create index fxx on foo(id)' statement to work.
Follow the steps:
```
cd cpsc5300
rm -f foo-fxx.db
```
Then continue running the remaining statements.
