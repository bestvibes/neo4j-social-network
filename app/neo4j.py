#imports
from py2neo import neo4j, node, rel
import time
from error_objects import *
from global_vars import *

#Connect to and clear the database
g = neo4j.GraphDatabaseService()
#schema = g.schema
#schema.create_index(user_label, mekid_key)
users_index = g.get_or_create_index(neo4j.Node, "Users")

def get_time():
    """ Return the time from the epoch in seconds

        Returns:
            (int): the time from the epoch in seconds
            
    """
    return int(time.time())

def create_user(name, mekid, **kwargs):
    """ Create new user
    
        Args:
            name (str): the name of the user to be added
            mekid (str): the mekid of the new user
            **kwargs (dict): dictionary of user network properties
        Returns:
            
        Raises:
            UserError: if user with same mekid already exists in graph database
            
    """
    batch = neo4j.WriteBatch(g)
    user = batch.create(node({name_key : name, mekid_key : mekid}))
    batch.set_property(user, time_key, get_time())
    for key, value in kwargs.iteritems():
        batch.set_property(user, key, value)
    batch.add_labels(user, user_label)
    batch.add_to_index_or_fail(neo4j.Node, users_index, mekid_key, mekid, user)
    results = batch.submit()
    
    #Check if user with mekid already exists
    if results[0] != results[-1]:
        raise UserError("User with mekid already Exists")
    
def get_user(mekid):
    """ Find user by mekid
    
        Args:
            mekid (str): Mekid to lookup
        Returns:
            (py2neo.neo4j.Node): Node of user with corresponding mekid
        Raises:
            UserError: if user node does not exist in graph database
            
    """
    try:
        return users_index.get(mekid_key, mekid)[0]
    except:
        raise UserError("User does not exist")

def common_networks(*mekids):
    """ Find common networks between list of users
    
        Args:
            mekids (list): List of mekids to find common networks between
        Returns:
            results (list): List of common networks e.g. [u'Twitter', u'Facebook', u'Instagram', u'Snapchat']
            
    """
    #get networks of first user
    user_props = get_network_properties(get_user(mekids[0]))
    results = (i for i in user_props.keys() if user_props[i] != no_network_value)
    
    #"bitwise and" each user with previous user to get common networks
    for mekid in mekids[1:]:
        user_props = get_network_properties(get_user(mekid))
        results = list(set(results) & set(i for i in user_props.keys() if user_props[i] != no_network_value))
    
    return results

def filter_dict_with_list(dictionary, key_list):
    """ Filter dictionary to keep keys from given list
    
        Args:
            dictionary (dict): dictionary to filter
            key_list (list): keys to keep in filtered dictionary
        Returns:
            dictionary (dict): the filtered dictionary with keys from input list
            
    """
    #Generate list of unwanted keys
    unwanted = (set(dictionary.keys()) - set(key_list))
    
    #Delete keys from dictionary
    for unwanted_key in unwanted: del dictionary[unwanted_key]
    
    return dictionary

def set_dict_to_zero_with_list(dictionary, key_list):
    """ Set dictionary keys from given list value to zero
    
        Args:
            dictionary (dict): dictionary to filter
            key_list (list): keys to turn zero in filtered dictionary
        Returns:
            dictionary (dict): the filtered dictionary with keys from input list turned to zero
            
    """
    #Generate list of unwanted keys
    unwanted = (set(dictionary.keys()) - set(key_list))
    
    #Delete keys from dictionary
    for unwanted_key in unwanted: dictionary[unwanted_key] = 0
    
    return dictionary
    
def clear_pending_requests(from_mekid, to_mekid):
    """ Clear any already accepted requests between two users
    
        Args:
            from_mekid: mekid of first user
            to_mekid: mekid of second user
            
    """
    #Get Users
    from_user = get_user(from_mekid)
    to_user = get_user(to_mekid)
    
    #Look for existing request relationships
    req_rels = list(g.match(start_node = from_user, rel_type = request_rel_type, end_node = to_user, bidirectional = True))
    num_req_rels = sum(1 for _ in req_rels)
    
    #Find friend relationship
    friend_rel = g.match_one(start_node = from_user, rel_type = friend_rel_type, end_node = to_user, bidirectional = True)
    
    if num_req_rels == 0:
        #No pending requests
        return
    elif num_req_rels == 1:
        #Request from only one user
        if friend_rel:
            req_rel = req_rels[0]
            friend_props = get_network_properties(friend_rel)
            req_rel_props = get_network_properties(req_rel)
            for network, value in req_rel_props.iteritems():
                if network in friend_props.keys():
                    if friend_props[network] == 1:
                        req_rel_props[network] = 0
            #Either delete or update request relationship
            if all(value == 0 for value in req_rel_props.values()):
                req_rel.delete()
            else:
                req_rel.update_properties(req_rel_props)
    elif num_req_rels == 2:
        #Process properties to look for request collisions and automatically accept
        req_rel_0 = req_rels[0]
        req_rel_props = get_network_properties(req_rel_0)
        common_req_props = [i for i in req_rel_props.keys() if req_rel_props[i] == 1]
        req_rel_1 = req_rels[1]
        second_req_rel_props = get_network_properties(req_rel_1)
        common_req_props = list(set(common_req_props) & set(i for i in second_req_rel_props.keys() if second_req_rel_props[i] == 1))
        if friend_rel:
            #Update friend relationship properties with collisions
            for network in common_req_props:
                friend_rel[network] = 1
                req_rel_0[network] = 0
                req_rel_1[network] = 0
            
            #Delete empty requests
            if all(value == 0 for value in req_rel_props.values()):
                    req_rel_0.delete()
            if all(value == 0 for value in second_req_rel_props.values()):
                    req_rel_1.delete()
            
            #Remove any redundant requests
            friend_props = get_network_properties(friend_rel)
            for req_rel in req_rels:
                req_rel_props = get_network_properties(req_rel)
                for network, value in req_rel_props.iteritems():
                    if network in friend_props.keys():
                        if friend_props[network] == 1:
                            req_rel_props[network] = 0
                #Either delete or update request relationship
                if all(value == 0 for value in req_rel_props.values()):
                    req_rel.delete()
                else:
                    req_rel.update_properties(req_rel_props)
        else:
            #Make new friendship with collisions
            batch = neo4j.WriteBatch(g)
            friend_rel = batch.create(rel(to_user, friend_rel_type, from_user))
            batch.set_property(friend_rel, time_key, get_time())
            for network in common_req_props:
                batch.set_property(friend_rel, network, 1)
                req_rel_0[network] = 0
                req_rel_1[network] = 0
            for network in networks:
                if network not in common_req_props:
                    batch.set_property(friend_rel, network, 0)
            batch.run()
            
            #Delete empty requests
            if all(value == 0 for value in get_network_properties(req_rel_0).values()):
                    req_rel_0.delete()
            if all(value == 0 for value in get_network_properties(req_rel_1).values()):
                    req_rel_1.delete()

def get_network_properties(rel_or_node):
    """ Get network properties of a node or relationship
    
        Args:
            rel_or_node (py2neo.neo4j.Node OR py2neo.neo4j.Relationship): the node or relationship to obtain properties from
        Returns:
            props (dict): dictionary with the network properties
            
    """
    props = rel_or_node.get_properties()
    props = filter_dict_with_list(props, networks)
    return props
    
def send_request(from_mekid, to_mekid, **kwargs):
    """ Send request from one user to another
    
        Args:
            from_mekid (str): mekid of the user sending the request
            to_mekid (str): mekid of the user receiving the request
            **kwargs (dict): dictionary of the properties of the request
        Raises:
            RequestError: if no networks requested
            
    """
    #Get user nodes
    from_user = get_user(from_mekid)
    to_user = get_user(to_mekid)
    
    if not kwargs:
        raise RequestError("No networks requested")

    #Verify that requested network properties exist for both users
    rel_props = filter_dict_with_list(kwargs, common_networks(from_mekid, to_mekid))
    
    #Add time of request
    rel_props[time_key] = get_time()
    
    #Check if request relation already exists
    req_rel = g.match_one(start_node = from_user, rel_type = request_rel_type, end_node = to_user, bidirectional = False)

    if req_rel:
        #It exists, update properties
        req_rel_props = get_network_properties(req_rel)
        for key, value in rel_props.iteritems():
            if value == 1 and key in req_rel_props.keys():
                req_rel_props[key] = 1
        req_rel.update_properties(req_rel_props)
    else:
        #Doesn't exist, make new relationship
        batch = neo4j.WriteBatch(g)
        req_rel = batch.create(rel(from_user, request_rel_type, to_user))
        for key, value in rel_props.iteritems():
                batch.set_property(req_rel, key, value)
        batch.run()
    
    clear_pending_requests(from_mekid, to_mekid)

def accept_request(to_mekid, from_mekid, **networks_accepted):
    """ Accept a request to konnect between two users
    
        Args:
            to_mekid (str): the mekid of the accepting user
            from_mekid (str): the mekid of the user who originally sent the request
            **networks_accepted(dict): dictionary of the networks the user is accepting or declining to konnect on
        Returns:
            
        Raises:
            LookupError: if no pending requests between users
            RequestError: if no networks accepted
            
    """
    #Get Users
    from_user = get_user(from_mekid)
    to_user = get_user(to_mekid)
    
    #Make properties for the new or updated friend relationship, make sure the networks are in common and valid networks
    networks_accepted = filter_dict_with_list(networks_accepted, common_networks(from_mekid, to_mekid))
    
    #Get the request relationship
    req_rel = g.match_one(start_node = from_user, rel_type = request_rel_type, end_node = to_user, bidirectional = False)
    
    #If relationship does not exist, raise error
    if not req_rel:
        raise LookupError("Could not find existing konnect request")
    
    if not networks_accepted:
        raise RequestError("No networks accepted")
    
    #Get request relationship network properties
    req_rel_props = get_network_properties(req_rel)
    
    #Get the friend relationship
    friend_rel = g.match_one(start_node = from_user, rel_type = friend_rel_type, end_node = to_user, bidirectional = True)

    if all(value != req_accept for value in req_rel_props.values()):
        req_rel.delete()
        
        #Delete redundant requests
        clear_pending_requests(from_mekid, to_mekid)
        
        return
    
    #Check for existing friend relationship
    if friend_rel:
        #It exists, update its properties
        for network, value in networks_accepted.iteritems():
            if req_rel_props.get(network, "") == 1:
                if value == req_accept:
                    friend_rel.update_properties({network : 1})
                req_rel_props[network] = 0
    else:
        #It doesn't exist, make a new relationship
        batch = neo4j.WriteBatch(g)
        friend_rel = batch.create(rel(from_user, friend_rel_type, to_user))
        batch.set_property(friend_rel, time_key, get_time())
        for network, value in networks_accepted.iteritems():
            if req_rel_props.get(network, "") == 1:
                if value == req_accept:
                    batch.set_property(friend_rel, network, 1)
                else:
                    batch.set_property(friend_rel, network, 0)
                req_rel_props[network] = 0
        batch.run()
    #Delete the request relationship if all requests taken care of
    if all(value == 0 for value in req_rel_props.values()):
        req_rel.delete()
    else:
        #Update the request relationship with latest properties
        req_rel.update_properties(req_rel_props)

    #Delete redundant requests
    clear_pending_requests(from_mekid, to_mekid)

def clear_db():
    g.clear()

if __name__ == '__main__':
    clear_db()
    
    create_user("Vaibhav Aggarwal", "bestvibes", **{fb_key : "vaibs", twit_key : "aggy", ig_key : "bestvibesss", sc_key : "bestvibess"})
    create_user("John Smith", "johnsmith", **{fb_key : "johns", twit_key : "johnny", ig_key : "smithy", sc_key : "jsmith"})
    send_request("bestvibes", "johnsmith", **{fb_key : 0, twit_key : 1, ig_key : 1, sc_key : 0})
    send_request("johnsmith", "bestvibes", **{fb_key : 0, twit_key : 1, ig_key : 0, sc_key : 1})
    accept_request("vibstaa", "bestvibes", **{twit_key:req_accept, ig_key:req_decline, sc_key:req_accept})
    clear_pending_requests("johnsmith", "bestvibes")

    print "connected and executed"