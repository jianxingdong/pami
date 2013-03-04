#Computes an approximate embedding based on Greedy Nystrom

#same params as BasicNystrom
#c number of groups
GreedyNystrom <- function(X,m, kernelFunc,k,c){
  K <- FullKernel(X, kernelFunc)
  n <- dim(X)[2]    
  #compute rand perm matrix
  rndGroups <- sample(c,n,replace=TRUE)
  
  G <- matrix(nrow=n,ncol=c)
  for(j in 1:c){
    G[,j] <- colSums(K[rndGroups==j,])
  }
  #init f and g
  f <- rowSums(G^2)    
  g <- diag(K)
  
  selectedCols <- c()
  W <- matrix(nrow=n,ncol=0)
  V <- matrix(nrow=c,ncol=0)
  #selecting columns
  for(t in 1:m){
    scores <- f/g  
    scores[selectedCols] <- scores[is.nan(scores)] <- scores[is.infinite(scores)] <- 0
    q <- which.max(scores)    
    
    if(scores[q]==0){
      print("max score is zero")
      break;  
    }    
    
    delta <- K[,q]
    gam <- G[q,]
    if(t>1){
      delta <- delta - W %*% W[q,]
      gam <- gam - V %*% W[q,]
    }
    alphaSqrt <- sqrt(delta[q])
    w <- delta/alphaSqrt
    v <- gam/alphaSqrt
    #update f and g
    r2 <- 0
    if(t==1){
      r2 <- W %*% t(v%*%V)
    }      
    
    if(sum(delta^2)<1e-10 || alphaSqrt<1e-10){
      print("t > rank(K)")
      break
    }
  
    r1 <- G %*% v
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
  e <- eigen(crossprod(W))
  Y <- W%*%e$vectors[,1:k]
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

kernelFunc <- polydot(degree = 1, scale = 1, offset = 0)
Y <- GreedyNystrom(t(test$x[1:100,]),10,kernelFunc,5,20)
