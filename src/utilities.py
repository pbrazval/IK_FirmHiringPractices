import numpy as np

def logoneplus(x):
    return np.log(1 + x)

def add_anchors(db):
    db['msa_anchor_city'] = db['dice_metro'].str.split("-", n=1, expand=True)[0].str.split(",", n=1, expand=True)[0]
    db['msa_anchor_state'] = db['dice_metro'].str.split(",", n=1, expand=True)[1].str.split("-", n=1, expand=True)[0]
    db['msa_anchor_state'] = db['msa_anchor_state'].str.strip()
    return db

def simplify_dice_metro(db):
    db = add_anchors(db)
    db['dice_metro'] = db['msa_anchor_city'] + "," + db['msa_anchor_state']
    return db