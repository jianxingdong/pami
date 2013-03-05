#dataset MNIST 1:4000 samples of the test data
X <- test$x[1:1000,]
numClusters <- 10
maxItrs <- 1000
#1- K-Means 
#result <- kmeans(X, numClusters, maxItrs, algorithm="Lloyd")
#kmeansLabels <- result$cluster

rbfSigma <- 0.2
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
NY <- BasicNystrom(t(X),colSampleSize,kernelFun,numFeatures)
#Y <- t(Y)
#result <- kmeans(Y, numClusters, maxItrs, algorithm="Lloyd")
#nystromLabels <- result$cluster

numParts <- 100
#5- K-means on Greedy Nystrom-based Embedding
#dominated my the time of computing the kernel
#Y <- GreedyNystrom(t(X),colSampleSize,kernelFun,numFeatures, numParts)
#result <- kmeans(Y, numClusters, maxItrs, algorithm="Lloyd")
#greedyNystromLabels <- result$cluster
