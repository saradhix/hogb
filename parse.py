import sqlparse
from sqlparse.tokens import Keyword
def main():
    sql='select i,   j,  sum(j ), max(j), min(k), count(l) from t1 group by i,  j order by i desc'
    extract_group_by(sql)
def extract_group_by(sql):
    res = sqlparse.parse(sql)

    stmt = res[0]
    gb_found = False
    for token in stmt.tokens:
        #print token, token.ttype, token.value
        if token.ttype is Keyword and token.value.upper() == 'GROUP':
            gb_found = True
    if gb_found:
        print "Group by found"
    else:
        print "No Group by"

    tokens = [i.value for i in stmt.tokens if not i.is_whitespace()]
    types = [i.ttype for i in stmt.tokens]
    print tokens
    print types
    group_by_col = None
    for idx, col in enumerate(tokens):
        print idx, col
        if tokens[idx] == 'group' and tokens[idx+1]=='by':
            group_by_col = tokens[idx+2]
    print "Group by is ", group_by_col

if __name__ == '__main__':
    main()
