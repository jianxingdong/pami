#Computes an approximate embedding based on Greedy Nystrom

#same params as BasicNystrom
#c number of groups
GreedyNystrom <- function(X,m, kernelFunc,k,c){
  K <- FullKernel(X, kernelFunc)
  n <- dim(X)[2]  
  #compute rand perm matrix
  rndGroups <- sample(c,n,replace=TRUE)
  G <- matrix(nrow=c,ncol=n)
  for(j in 1:c){
    G[j,] <- rowSums(K[,rndGroups=j])
  }
  #init f and g
  f <- apply(G,2,function(x){sum(x^2)})
  g <- diag(K)
  selectedCols <- c()
  
  #selecting columns
  for(t in 1:m){
    q <- which.max(f/g)
    selectedCols <- c(selectedCols, q)
    delta <- K[,q] - 
  }
}

#computes the full kernel matrix
#TODO use a better implementation and a better storage
FullKernel <-function(X,kernelFunc){
  n <- dim(X)[2]
  fullKernel <- matrix(nrow=n, ncol=n)
  for(i in 1:n){
    for(j in 1:i){
      fullKernel[i,j] <- fullKernel[j,i] <- kernelFunc(X[,i], X[,j])
    }
  }
  return(fullKernel)
}
