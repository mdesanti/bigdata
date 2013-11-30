class FlownMilesSerializer < ActiveModel::Serializer
  attributes :year, :airline_code, :airline_name, :miles
end