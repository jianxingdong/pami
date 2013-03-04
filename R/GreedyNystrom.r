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
  W <- matrix(nrow=n,ncol=0)
  V <- matrix(nrow=c,ncol=0)
  #selecting columns
  for(t in 1:m){
    scores <- f/g
    scores[selectedCols] <- scores[is.nan(scores)] <- scores[is.infinite(scores)] <- 0
    q <- which.max(f/g)    
    
    if(scores[q]==0){
      print("max score is zero")
      break;  
    }    
    
    delta <- ifelse(t==1, K[,q], K[,q] - W %*% W[q,])
    gam <- ifelse(t==1, G[,q], G[,q] - V %*% W[q,])        
    alphaSqrt = sqrt(delta[q])
    w <- delta/alphaSqrt
    v <- gam/alphaSqrt
    #update f and g
    r2 <- ifelse(t==1,0, W %*% (v%*%V))
    
    if(sum(delta^2)<1e-10 || alphaSqrt<1e-10){
      print("t > rank(K)")
      break
    }
  
    r1 <- V%*%v
    r3 <- w^2
    
    f <- f - 2*(w * (r1 - r2)) + (sum(v^2))*r3
    g <- g - r3
    
    f[q] <- g[q] <- f[f<1e-10] <- g[g<1e-10] <- 0    
    
    
    #updates for the next iterations
    W <- cbind(W,w)
    V <- cbind(V,v)    
    selectedCols <- c(selectedCols, q)
  }
  #compute the embedding
  e <- eigenn(W%*%t(W))
  Y <- t(e$vectors[,1:k])%*%W
  return(Y)
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
