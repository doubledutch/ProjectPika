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

## X. Known needed improvements

### Column.java:knownFreePageId

Currently we could leave big gaps in pages when looking for first fit for a large object.... improve!

### Page.java:saveChanges

Make the right choice between flushing full pages and writing individual diffs... profile and choose!

### Variant.java:getSize

Due to the way we implemented DELETE markers in pages, the size of variants can't be trusted to calculate offsets.

