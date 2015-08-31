# ProjectPika
A column oriented database experiment

````
{
	"id":928374,
	"username":"kasper",
	"prefs":{
		"color":"#F8A034",
		"pageSize":20
	},
	"data":[
		5,32489,23
	]
}
````
This object would result in the following keys:

````
id
username
prefs.color
prefs.pagesize
data.0
data.1
data.2
````