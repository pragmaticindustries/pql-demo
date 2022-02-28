# PQL proof-of-concept Demo

Hi all,
this is first off a discussion concenring the Digital Cockpit (partially) as well as the Portal Team, so all devs, basically.

We had several discussions recently around e.g.

    Werkzeugverwaltung
    Mischungen
    Verbrauchsmaterial
    Digitales Sägeblatt
    Werkzeuge in DCL

And more generally we are currently lacking a bit of generalization or generalibility (???) in our FactoryHub Apps (IMHO).

So I thought a lot about
1) The Datamodel (fixed but flexible)
2) How could we have a suitable SPI that the Views are fixed and via an easy configuration one can say what to query and how things are related

Some Motivating Examples:
In the "Digitales Sägeblatt" we e.g. Track each Sawblade that is equipped on a machine and then want to display it with other metrics like

    How many cuts did it do?
    How long was it equipped?
    How was the productivity while equipped?
    Which materials were on the machine while it was equipped?

And so on...

At Laempe we have other requirements. For each Cycle we e.g. want to know

    What tool was used for that cycle and how many engravings (or cavities) did it have?
    What were the mixtures (from the mixer) that were produced for that machine previous to the cycle
    How many cycles were done for one respective tool?
    How often was the tool equipped?

and so on...

Our current approach is to model this using (a bit) general tables / Models / Entities in the databse and then doing the queries "hard" either in Java or Django via the respective ORMs.

And this is a fine approach if we have clear requirements (as we have now) but it will break pretty fast as soon as we try to e.g. generalize the FactoryHub to make it usable with a other types of Machines.

So how could a solution look like?
First, its not easy as its a complex (or interesting!) problem.
The first thought I had (and have since a while) is to generalize our "scraping" rules as most of the data comes from PLCs.

Therefore a first class citizen in our database should be "generalized cycles" (already discussed many times here) which consist of

    A cycle type (machine-cycle, mixer-mixture, period-where-one-sawblade-was-equpped, ...)
    A start and an end
    The source machine
    Arbitrary many further properties as JSON / dict

The scraping rule is pretty similar to what Tim Mitsch Tim already does in some scenarios.
We watch one trigger (e.g. when this field changes) and then read all of the remeaining fields (and write the start). When the trigger is triggered the next time we write the end and then the new entry.

This would allow us to model all of the above mentioned examples in the same way from a Database and algorithm / scraping perspective, so pure configuration here. Yay. 🤩

But whats left is how to handle relations between these objects and how to make the UI able to represent these complex relations?

Here is where the subject "Portal Query Language" comes in (PQL). Of course this is an ironic term as there are already way too many SQL-Like Query Languages...

Back to topic, what if we could simply express relations between these objects in a simple Language (or Editor or whatever) that allows us to represent the relations between these objects also as CONFIGURATION. This is where the magic comes in. 🧙‍♂️

Consider the Snippet:

SELECT t.name, 
  COUNT(SELECT c FROM Cycles [WHERE t.start <= c.start AND c.start < t.end]),
  LIST(SELECT m.material FROM Materials [WHERE t.start <= m.start AND m.start < t.end]) 
FROM Tools

(which is my toy example from the Sawblade-thing).

It would say that the scraper generated 3 types of "objects", Tools (Sägeblatt), Cycles (Schnitte), Materials (Produkte).
And it would tell us a bit about their relation.
We want a table (all results here should be scalars or tables, I guess) which has one entry for each Tool (Sägeblatt).
Futhermore, its first Column is the name of the Sägeblatt.
The second column is the number of cycles whose start-time is WHILE the Sägeblatt was on (between its start and end). You may notice the part in the square brackets that I would make optional. By default all joins should be done by time!
The third column would be a list of all Materials that were on the machine while the sawblade was on. As a list of their names only. Not Unique yet, this would need another command perhaps.

So why not use SQL for that?
Yes, why?
In fact all I did above is only a small, small subset of SQL (in fact these are nested Queries).
All I did was to make the syntax a bit easier so that it is (perhaps not End-User firendly) technically affine people friendly.

I will play around a bit more with this idea but from my current understanding we could use this approach to end in a situation where we could make complete Views ONLY controlled by a set of these queries and some further rules like what filters to present and such.

I think this would be pretty cool (if possible) and make our stuff extremely adaptive.
