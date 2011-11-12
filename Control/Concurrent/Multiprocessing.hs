module Control.Concurrent.Multiprocessing
    (parallel, parallel_, parForM, parForM_, splitIntoBitsOf)
    where

import Prelude hiding (catch)
import Data.Either (partitionEithers)
import Control.Exception
import Control.Monad
import Control.Concurrent.MVar
import GHC.Conc
import Control.Concurrent.ParallelIO.Local hiding (parallel_)

splitIntoBitsOf _ [] = [[]]
splitIntoBitsOf n xs    | not $ null ys' = ys : (splitIntoBitsOf n ys')
                        | otherwise = [ys]
    where
        ys = take n xs
        ys' = drop n xs

-- edited from Control/Concurrent/ParallelIO/Local.hs because original has space leak (defined as `parallel pool xs >> return ()`)
parallel_ :: Pool -> [IO a] -> IO ()
parallel_ pool acts = mask $ \restore -> do
        main_tid <- myThreadId
        resultvars <- forM acts $ \act -> do
            resultvar <- newEmptyMVar
            _tid <- forkIO $ bracket_ (killPoolWorkerFor pool) (spawnPoolWorkerFor pool) $
                reflectExceptionsTo main_tid $ do
                    res <- restore act
                    True <- tryPutMVar resultvar res
                    return ()
            return resultvar
        extraWorkerWhileBlocked pool (mapM_ takeMVar resultvars)
    where
        reflectExceptionsTo :: ThreadId -> IO () -> IO ()
        reflectExceptionsTo tid act = act `catch` \e -> throwTo tid (e :: SomeException)

parForM xs action = withPool numCapabilities $ \pool -> do
    let splitted = splitIntoBitsOf numCapabilities xs
    (errs, results) <- fmap (partitionEithers . concat) $
        (forM (map (map action) splitted) $ \s -> do
            rs <- parallelInterleavedE pool s
            rs `pseq` return rs)
    return $ (errs, results)

parForM_ xs action = withPool numCapabilities $ \pool -> do
    let splitted = splitIntoBitsOf numCapabilities xs
    (errs, _) <- fmap (partitionEithers . concat) $
        (forM (map (map action) splitted) $ \s -> do
            rs <- parallelInterleavedE pool s
            rs `pseq` return rs)
    print errs