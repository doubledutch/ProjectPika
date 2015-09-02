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

## X. Known needed improvements

### Column.java:knownFreePageId

Currently we could leave big gaps in pages when looking for first fit for a large object.... improve!
