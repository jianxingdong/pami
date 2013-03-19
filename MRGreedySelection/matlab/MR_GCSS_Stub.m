function [ S,W ] = MR_GCSS_Stub(target, source, k,numParts, incRatio)    
    [n] = size(source,2);

    %phase 1 selection 
    kp = ceil(k/numParts*(1+incRatio));
    partNumCols = ceil(n/numParts);
    from = 1;
    phase1Indexes = [];
    for mp = 1:numParts
        display(mp)
       to = from + partNumCols;
       if(mp==numParts)
           to = n;
       end
      [S]= GreedySelection(target, source(:,from:to), kp);
      phase1Indexes = [phase1Indexes; S+from-1];
      from = from + partNumCols ; 
    end
    phase1Indexes
   %phase 2 selection
   [S, W]= GreedySelection(target, source(:,phase1Indexes), k);
   S = phase1Indexes(S);
end
