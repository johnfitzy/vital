## Build
```
cd /path/to/vital
docker build -t vital:latest .
```
## Run
`docker run -it --rm --name vital -v /path/to/vital:/usr/src/app vital:latest`

## Output
```
/path/to/vital/result.csv.gz
zcat /path/to/vital/result.csv.gz 
gunzip /path/to/vital/result.csv.gz
```