# Project Pika

A column oriented database experiment.

## 1. Introduction

Pika is intended to be a small lightweight database for use in mobile applications.
It will be schemaless (but with the option to enforce partial schemas) and column oriented.

Much of the initial work in Pika will be focused on a JSON based data source. This is intended to ensure an easy way to integrate the database with a JSON based backend API. There will be an interface to query and receive plain objects later on - in fact, a secondary priority for the design of Pika is to allow fine grained notifications of data changes through a simple observer pattern.

Let's look at a sample JSON object for a user and see how this could be mapped onto columns.

````
{
	"id":928374,
	"username":"kasper.jeppesen",
	"firstName":"Kasper",
	"lastName":"Jeppesen",
	"prefs":{
		"color":"#F8A034",
		"pageSize":20
	}
}
````

The idea is to treat each key as a column and for nested objects to treat the entire path as an object. Thus, this object would result in the following columns:

````
id
username
firstName
lastName
prefs.color
prefs.pagesize
````

Since JSON object graphs are limited to trees - this is possible no matter the depth of the objects. The one problem it doesn't solve is arrays. One possible solution for this is to treat the indexes into an array as nothing more than numeric keys. Lets look at a sample for this:

````
{
	"foo":[3,"bar","baz"]
}
````

This could be mapped onto the following columns:

````
foo.0
foo.1
foo.2
````

However, since the purpose of columns in Pika is to hold simple values, this breaks down once we have an array holding objects. One way to fix this, could be to add a simple value type that is an object reference. In that way, the inner objects in an array could be stored as any other object in the columns.... this however, could have some serious performance implications when we are trying to read objects back from the columns - but that's something we will get to later :-)

## 2. Page Strategy

All columns are stored in a series of linked pages. All pages are stored in a single pagefile. Every page has a bloomfilter for the object id's contained in the page, an integer holding the current page fill and a pointer for the next page - everything else is payload in the form of a stream of variants.

I'm currently thinking about a change to a model where there is a page pointer in between each variant instead of a single next page pointer. This will allow us to build up a tree based structure of pages based on the page values... the question is, how do we sort values of different types stored in the same column?

````
4096 byte pages
bloom and fill header = 4088 payload
pointer, type and oid pr value

for 4 byte integer... 4+4+1+4 = 13 byte pr value = 160 values pr page
for 16 char string... 4+4+1+2+2*16 = 43 byte pr value = 95 values pr page

{
	"id":233992,
	"username":"kasper.jeppesen",
	"firstName":"Kasper",
	"lastName":"Jeppesen",
	"image":"/resources/image/233992-profile.png",
	"created":"2015-09-02 16:37"
}

161 bytes as raw json storage

228 byte in columns with 2 byte chars
````


MacBook-Pro:ProjectPika kasperjj$ java -jar build/libs/PikaDB-1.0.jar 
 + Writing 100000 objects
   - Write in 1552ms 64432 obj/s
   - Database size : 13084kb
 + Reading full objects - 100%
   - Read in 545ms 183486 obj/s
 + Reading full objects - 50%
   - Read in 327ms 152905 obj/s
 + Reading full objects - 25%
   - Read in 244ms 204918 obj/s
 + Reading full objects - 5%
   - Read in 204ms 245098 obj/s
 + Reading full objects - 1%
   - Read in 304ms 164473 obj/s
 + Reading partial objects
   - Read in 155ms 645161 obj/s
 + Reading a single object - early
   - Read in 0ms
 + Reading a single object - mid
   - Read in 16ms
 + Reading a single object - late
   - Read in 19ms
 + Updating a single object
   - Updated in 9ms
 + Deleting a single object
   - Deleted in 12ms

Post objectset refactor

MacBook-Pro:ProjectPika kasperjj$ java -jar build/libs/PikaDB-1.0.jar 
 + Writing 100000 objects
   - Write in 1456ms 68681 obj/s
   - Database size : 13084kb
 + Reading full objects - 100%
   - Read in 911ms 109769 obj/s
 + Reading full objects - 50%
   - Read in 338ms 147928 obj/s
 + Reading full objects - 25%
   - Read in 269ms 185873 obj/s
 + Reading full objects - 5%
   - Read in 199ms 251256 obj/s
 + Reading full objects - 1%
   - Read in 129ms 387596 obj/s
 + Reading partial objects
   - Read in 121ms 826446 obj/s
 + Reading a single object - early
   - Read in 1ms
 + Reading a single object - mid
   - Read in 14ms
 + Reading a single object - late
   - Read in 18ms
 + Updating a single object
   - Updated in 6ms
 + Deleting a single object
   - Deleted in 11ms


## X. Known needed improvements

### Column.java:knownFreePageId

Currently we could leave big gaps in pages when looking for first fit for a large object.... improve!

### Page.java:saveChanges

Make the right choice between flushing full pages and writing individual diffs... profile and choose!

### Variant.java:getSize

Due to the way we implemented DELETE markers in pages, the size of variants can't be trusted to calculate offsets.
Note: this has been fixed, but the fix is horrible....

### ObjectSet

I changed objectset over to not build objects until the end (thus allowing multiple object types instead of just JSONObject)
This created a performance penalty that gets larger the more objects... investigate and fix if possible.
