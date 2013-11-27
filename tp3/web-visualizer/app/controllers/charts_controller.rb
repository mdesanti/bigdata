class ChartsController < ApplicationController

  def index
  end

  def tweets_for
    render json: TweetCounter.all
  end
end