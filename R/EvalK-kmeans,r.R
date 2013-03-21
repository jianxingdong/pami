library(clue)

#dataset MNIST 1:4000 samples of the test data
X <- test$x[1:1000,]
numClusters <- 10
maxItrs <- 1000
#1- K-Means 
result <- kmeans(X, numClusters, maxItrs, algorithm="Lloyd")
kmeansLabels <- result$cluster

rbfSigma <- 1e-10
kernelFun <- rbfdot(sigma=rbfSigma)
#2- Kernel K-Means
#kkmeansLabels <- kkmeans(X, numClusters, kernelFun)

numFeatures <- 20

#3- K-means on RFF Embedding
#fcParams <- list()
#fcParams$sigma <- rbfSigma
#Y <- rffEmbedding(t(X),numFeatures/2,RBFFourierComponents, fcParams)
#Y <- t(Y)
#result <- kmeans(Y, numClusters, maxItrs, algorithm="Lloyd")
#RFFLabels <- result$cluster

colSampleSize <- 50

#4- K-means on Nystrom-based Embedding
#NY <- BasicNystrom(t(X),colSampleSize,kernelFun,numFeatures)
#NY <- t(NY)
#result <- kmeans(NY, numClusters, maxItrs, algorithm="Lloyd")
#nystromLabels <- result$cluster

numParts <- 100
#5- K-means on Greedy Nystrom-based Embedding
#Y <- GreedyNystrom(t(X),colSampleSize,kernelFun,numFeatures, numParts)
#result <- kmeans(Y, numClusters, maxItrs, algorithm="Lloyd")
#greedyNystromLabels <- result$cluster

#NMI Computations
#p1 <- as.cl_partition(test$y[1:1000])
#p2 <- as.cl_partition(kkmeansLabels)
#p3 <- as.cl_partition(RFFLabels)
#p4 <- as.cl_partition(nystromLabels)
#p5 <- as.cl_partition(greedyNystromLabels)
#p6 <- as.cl_partition(kmeansLabels)
#clEns <- cl_ensemble(p1,p2,p3,p4,p5,p6)
#cl_agreement(clEns,method="NMI")