import math
fr = open('docword.pubmed.txt','r')
fw = open('pubmed-full.txt','w')
numDocs = 8200000
numWords = 141043
chunkSize = 1000
for c in range(0,int(math.ceil(float(numWords)/chunkSize))):
    wordOffset = c*chunkSize + 1
    chunkSize = min(chunkSize, numWords - wordOffset + 1)
    vctrs = []
    for i in range (0, chunkSize):
        vctrs.append([0]*numDocs)       
    skip = 0
    for line in fr:
        if(skip<3):
            skip = skip +1
            continue
        parts = line.split()
        wordId = int(parts[1])
        docId = int(parts[0])
        if wordId >=wordOffset and wordId<wordOffset+chunkSize:    
            vctrs[wordId-wordOffset][docId-1] = int(parts[2])

    #writing the word vectors
    for i in range (0, chunkSize):
        fw.write(','.join(str(x) for x in vctrs[i])+'\n')
    
    fr.seek(0)
    print c
fw.close()
fr.close()