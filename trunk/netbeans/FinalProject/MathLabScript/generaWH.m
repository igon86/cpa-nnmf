function a = generaWH(dim,k,file,average)
    %S = sparse(zeros(dimx,dimy));
    %dim = dimx*dimy;
    %numelem = ceil(dim/sparsity)
    %fprintf(1,'dim: %d \nnumelem: %d\n',dim,numelem);
    %fflush(1);
    fid = fopen(file,'w');
    a=0;
    %s=0;
    tic
    for i =1: dim
        fprintf(fid,'%d\t',i);
        for j=1:k

            %if p <= ((numelem-s)/dim)
                
                x = 0;
                while x <= 0
                    x = randn()+average;
                end
                if j==k
                    fprintf(fid,'%f',x);
                else
                    fprintf(fid,'%f#',x);
                end    
            %end
        end
        fprintf(fid,'\n');
    end
    fclose(fid);
    toc
end
