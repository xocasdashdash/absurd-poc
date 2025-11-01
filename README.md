# Absurd POC

Been keeping an eye on https://github.com/earendil-works/absurd/ and got a client working. 

Interesing concept, the go client probably needs some TLC, it's based on the javascript one
vibecoded a go version.

Added some generics support but i'm not too sure about it, current example works:
- receives a webhook
- dispatches a task
- a task picks it up, dispatches another task
- a final task sends out the webhook based on some db configuration
- 
